package com.cirus.io;

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETResult;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETSentEvent;
import com.exacttarget.fuelsdk.ETSoapObject;

import java.util.List;

public final class  TrackingEventHelper {

  public static <T extends ETSoapObject> void retrieveEvents(ETClient client, Class<T> type, ETFilter filter){
    try {
      System.out.println("Start " + type);
      ETResponse<T> retrieve = null;
      retrieve = ETSentEvent.retrieve(client, type, null, null, filter);
      List<ETResult<T>> results = retrieve.getResults();
      System.out.println("Result " + retrieve.getResponseMessage() + " - " + type + " - " + results.size() );
    } catch (ETSdkException e) {
      e.printStackTrace();
    }
  }
}
