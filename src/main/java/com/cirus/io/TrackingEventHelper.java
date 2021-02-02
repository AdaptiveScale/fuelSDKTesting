package com.cirus.io;

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSoapObject;

import java.util.List;

public final class  TrackingEventHelper {

  public static <T extends ETSoapObject> void retrieveEvents(ETClient client, Class<T> type, ETFilter filter){
    try {
      System.out.println("Start " + type);
      ETResponse<T> retrieve = null;
      retrieve = ETSoapObject.retrieve(client, type, null, null, filter);
      List<ETResult<T>> results = retrieve.getResults();
//      console(results);
      System.out.println("Result " + retrieve.getResponseMessage() + " - " + type + " - " + results.size() );
    } catch (ETSdkException e) {
      e.printStackTrace();
    }
  }

  private static <T extends ETSoapObject> void console(List<ETResult<T>> rows) {
    System.out.println("-- beatify --");
    int counter=0;
    for (ETResult<T> row : rows) {
      System.out.print("Row ID " + counter++ + " ");
      System.out.println(row.toString());
    }
    System.out.println("-- beatify --");
  }
}
