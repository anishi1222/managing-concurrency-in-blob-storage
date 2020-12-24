package com.blobs.quickstart;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

public class BlobOperation {
    final String CONNECTION_STRING="CONNECTION_STRING";
    final String CONTAINER="BLOB_CONTAINER";
    BlobClient blobClient;

    public BlobOperation(){
        blobClient = new BlobClientBuilder()
                .connectionString(CONNECTION_STRING)
                .containerName(CONTAINER)
                .blobName("test1")
                .buildClient();
    }

    // Optimistic
    void optimistic() {
        try {
            // 1st
            String blobContents1 = "First update. Overwrite blob if it exists.";
            byte[] byteArray = blobContents1.getBytes(StandardCharsets.UTF_8);
            InputStream targetStream = new ByteArrayInputStream(byteArray);

            BlobParallelUploadOptions blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length);
            Response<BlockBlobItem> blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents1);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());
            String originalEtag = blobItemResponse.getValue().getETag();

            // 2nd
            String blobContents2 = "Second update overwrites first update.";
            byteArray = blobContents2.getBytes(StandardCharsets.UTF_8);
            targetStream = new ByteArrayInputStream(byteArray);

            blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length);
            blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents2);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());

            // 3rd ETagが一致する場合に上書き（書き換わっているから失敗する）
            String blobContents3 = "Third update. If-Match condition set to original ETag.";
            byteArray = blobContents3.getBytes(StandardCharsets.UTF_8);
            targetStream = new ByteArrayInputStream(byteArray);

            BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setIfMatch(originalEtag);
            blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length)
                    .setRequestConditions(blobRequestConditions);
            blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents3);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());
        }
        catch(BlobStorageException e) {
            if(e.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
                System.out.println("Precondition failure as expected. Blob's ETag does not match ETag provided.");
            }
            else {
                e.printStackTrace();
            }
        }
    }

    // Pessimistic
    void pessimistic() {

        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(blobClient).buildClient();
        try {
            // 1st
            String blobContents1 = "First update. Overwrite blob if it exists.";
            byte[] byteArray = blobContents1.getBytes(StandardCharsets.UTF_8);
            InputStream targetStream = new ByteArrayInputStream(byteArray);

            BlobParallelUploadOptions blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length);
            Response<BlockBlobItem> blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents1);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());

            // Leaseを取得
            String leaseId = blobLeaseClient.acquireLease(15);
            System.out.println("LeaseId: " + leaseId);

            // 2nd Lease IDを取得しているので成功するはず
            String blobContents2 = "Second update. Lease ID provided on request.";
            byteArray = blobContents2.getBytes(StandardCharsets.UTF_8);
            targetStream = new ByteArrayInputStream(byteArray);

            BlobRequestConditions blobRequestConditions = new BlobRequestConditions().setLeaseId(leaseId);
            blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length).setRequestConditions(blobRequestConditions);
            blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents2);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());


            // 3rd Lease IDを取得していないので失敗する
            String blobContents3 = "Third update. No lease ID provided.";
            byteArray = blobContents3.getBytes(StandardCharsets.UTF_8);
            targetStream = new ByteArrayInputStream(byteArray);

            blobParallelUploadOptions = new BlobParallelUploadOptions(targetStream, byteArray.length);
            blobItemResponse = blobClient.uploadWithResponse(blobParallelUploadOptions, null, Context.NONE);
            System.out.println("Content: " + blobContents3);
            System.out.println("Status: " + blobItemResponse.getStatusCode());
            System.out.println("ETag: " + blobItemResponse.getValue().getETag());

        }
        catch(BlobStorageException e) {
            if(e.getStatusCode() == HttpURLConnection.HTTP_PRECON_FAILED) {
                System.out.println("Precondition failure as expected. The lease ID was not provided.");
            }
            else {
                e.printStackTrace();
            }
        }
        finally {
            blobLeaseClient.releaseLease();
        }
    }
}
