package com.blobs.quickstart;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class AppTest {
    private final PrintStream originalErr = System.err;
    private ByteArrayOutputStream errorOutput;

    @Before
    public void redirectStandardError() {
        errorOutput = new ByteArrayOutputStream();
        System.setErr(new PrintStream(errorOutput, true, StandardCharsets.UTF_8));
    }

    @After
    public void restoreStandardError() {
        System.setErr(originalErr);
    }

    @Test
    public void main_shouldPrintUsageWhenNoArgumentsAreProvided() {
        App.main(new String[0]);

        assertEquals(usageMessage(), errorOutput.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void main_shouldPrintUsageWhenMultipleArgumentsAreProvided() {
        App.main(new String[] {"OPTIMISTIC", "PESSIMISTIC"});

        assertEquals(usageMessage(), errorOutput.toString(StandardCharsets.UTF_8));
    }

    private String usageMessage() {
        return "[Usage] java com.blobs.quickstart.App OPTIMISTIC|PESSIMISTIC" + System.lineSeparator();
    }
}