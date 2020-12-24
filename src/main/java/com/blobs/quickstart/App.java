package com.blobs.quickstart;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        if(args.length !=1) {
            System.err.println("[Usage] java com.blobs.quickstart.App OPTIMISTIC|PESSIMISTIC");
            return;
        }
        BlobOperation blobOperation = new BlobOperation();
        // Optimistic or Pessimistic, or last wins
        switch(args[0].toUpperCase()) {
            case "OPTIMISTIC":
                blobOperation.optimistic();
                 break;
            case "PESSIMISTIC":
                blobOperation.pessimistic();
                break;
            default:
                break;
        }
    }
}
