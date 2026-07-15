package com.blobs.quickstart;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;

public class BlobOperationTest {
    private final PrintStream originalOut = System.out;
    private ByteArrayOutputStream standardOutput;

    @Before
    public void redirectStandardOutput() {
        standardOutput = new ByteArrayOutputStream();
        System.setOut(new PrintStream(standardOutput, true, StandardCharsets.UTF_8));
    }

    @After
    public void restoreStandardOutput() {
        System.setOut(originalOut);
    }

    @Test
    public void optimistic_shouldAttachConditionsToThirdUpload() {
        BlobClient blobClient = mock(BlobClient.class);
        Response<BlockBlobItem> firstResponse = responseWithEtag("etag-1");
        Response<BlockBlobItem> secondResponse = responseWithEtag("etag-2");
        Response<BlockBlobItem> thirdResponse = responseWithEtag("etag-3");
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
            .thenReturn(firstResponse)
            .thenReturn(secondResponse)
            .thenReturn(thirdResponse);

        new BlobOperation(blobClient).optimistic();

        ArgumentCaptor<BlobParallelUploadOptions> uploadOptions = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
        verify(blobClient, times(3)).uploadWithResponse(uploadOptions.capture(), isNull(), eq(Context.NONE));

        List<BlobParallelUploadOptions> uploads = uploadOptions.getAllValues();
        assertNull(uploads.get(0).getRequestConditions());
        assertNull(uploads.get(1).getRequestConditions());
        assertNotNull(uploads.get(2).getRequestConditions());
    }

    @Test
    public void optimistic_shouldReportExpectedMessageWhenIfMatchFails() {
        BlobClient blobClient = mock(BlobClient.class);
        Response<BlockBlobItem> firstResponse = responseWithEtag("etag-1");
        Response<BlockBlobItem> secondResponse = responseWithEtag("etag-2");
        when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
            .thenReturn(firstResponse)
            .thenReturn(secondResponse)
                .thenThrow(preconditionFailedException());

        new BlobOperation(blobClient).optimistic();

        assertTrue(standardOutput.toString(StandardCharsets.UTF_8)
                .contains("Precondition failure as expected. Blob's ETag does not match ETag provided."));
    }

            @Test
            public void pessimistic_shouldReleaseLeaseAndReportExpectedMessageWhenLeaseIsMissing() {
            BlobClient blobClient = mock(BlobClient.class);
            BlobLeaseClient blobLeaseClient = mock(BlobLeaseClient.class);
            Response<BlockBlobItem> firstResponse = responseWithEtag("etag-1");
            Response<BlockBlobItem> secondResponse = responseWithEtag("etag-2");
            BlobStorageException preconditionFailure = preconditionFailedException();
            when(blobLeaseClient.acquireLease(15)).thenReturn("lease-id");
            when(blobClient.uploadWithResponse(any(BlobParallelUploadOptions.class), isNull(), eq(Context.NONE)))
                .thenReturn(firstResponse)
                .thenReturn(secondResponse)
                .thenThrow(preconditionFailure);

            new BlobOperation(blobClient, blobLeaseClient).pessimistic();

            ArgumentCaptor<BlobParallelUploadOptions> uploadOptions = ArgumentCaptor.forClass(BlobParallelUploadOptions.class);
            verify(blobClient, times(3)).uploadWithResponse(uploadOptions.capture(), isNull(), eq(Context.NONE));
            verify(blobLeaseClient).acquireLease(15);
            verify(blobLeaseClient).releaseLease();

            List<BlobParallelUploadOptions> uploads = uploadOptions.getAllValues();
            assertNull(uploads.get(0).getRequestConditions());
            assertNotNull(uploads.get(1).getRequestConditions());
            assertNull(uploads.get(2).getRequestConditions());
            assertTrue(standardOutput.toString(StandardCharsets.UTF_8)
                .contains("Precondition failure as expected. The lease ID was not provided."));
            }

    @SuppressWarnings("unchecked")
    private Response<BlockBlobItem> responseWithEtag(String eTag) {
        Response<BlockBlobItem> response = mock(Response.class);
        BlockBlobItem blobItem = mock(BlockBlobItem.class);
        when(response.getStatusCode()).thenReturn(HttpURLConnection.HTTP_CREATED);
        when(response.getValue()).thenReturn(blobItem);
        when(blobItem.getETag()).thenReturn(eTag);
        return response;
    }

    private BlobStorageException preconditionFailedException() {
        HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusCode()).thenReturn(HttpURLConnection.HTTP_PRECON_FAILED);
        return new BlobStorageException("Precondition failed", response, null);
    }
}