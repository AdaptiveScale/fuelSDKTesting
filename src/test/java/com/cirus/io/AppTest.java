package com.cirus.io;

import com.exacttarget.fuelsdk.ETBounceEvent;
import com.exacttarget.fuelsdk.ETClickEvent;
import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETOpenEvent;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.exacttarget.fuelsdk.ETUnsubEvent;
import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest {
  private static ETClient client = null;
  private static String id = "";
  private static int debugIndex;

  @Before
  public void setup() throws ETSdkException {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ALL);
    client = new ETClient("");
  }

  @Test
  public void testDatExtractPagination() {
      dataExtraction(new ETFilter());
  }

  @Test
  public void testDatExtractFilterAndPagination() {
      debugIndex = 0;
      ETExpression etExpression = new ETExpression();
      etExpression.setOperator(ETExpression.Operator.EQUALS);
      etExpression.setProperty("pet_name");
      etExpression.setValue("Reba");

      ETFilter etFilter = new ETFilter();
      etFilter.setProperties(Lists.newArrayList("pet_gender",
                                                "reproductive_status",
                                                "source_of_pet",
                                                "weight_lb",
                                                "birth_dt",
                                                "pet_name",
                                                "favorite_brand",
                                                "age",
                                                "health_interests",
                                                "alreadyexists",
                                                "pet_create_dt",
                                                "contact_id",
                                                "pet_modify_dt",
                                                "abilitec id",
                                                "species",
                                                "shelter_name",
                                                "pet_id",
                                                "breed_desc",
                                                "breed_cd"
      ));
      etFilter.setExpression(etExpression);
      dataExtraction(etFilter);
  }

  private static void dataExtraction(ETFilter etFilter) {
      long startTime = System.currentTimeMillis();
    try {
      ETResponse<ETDataExtensionRow> response = CustomETDataExtensionRow.select(client, "key=" + id, etFilter);
      System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
      System.out.println(response.getTotalCount() + " " + response.getPageSize());
      List<ETDataExtensionRow> rows = response.getObjects();
      console(rows);


      while (response.getResponseMessage().equals("MoreDataAvailable")) {
        System.out.println("----------------- Next -------------");
        response = CustomETDataExtensionRow.continueRequest(client, null, response.getRequestId(), etFilter);
        System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
        System.out.println(response.getTotalCount() + " " + response.getPageSize());
        console(response.getObjects());
      }

      long endTime = System.currentTimeMillis();
      System.out.println("Total Time " + (endTime - startTime) / 1000);

    } catch (ETSdkException ex) {
      System.out.println(ex);
    } finally {
      debugIndex = 0;
    }
  }

  private static void console(List<ETDataExtensionRow> rows) {
    for (ETDataExtensionRow row : rows) {
      System.out.print("Row ID " + debugIndex++ + " ");
      for (String colName : row.getColumnNames()) {
        System.out.print(colName + " " + row.getColumn(colName) + " ** ");
      }
      System.out.println();
    }
  }

  @Test
  public void testTrackingEvent() {
      //todo unable to find event for
      // 1. Not Sent
      //    Send Jobs
    //     Spam Complaints
    //     Status Changes
    //     SMS Message Tracking
    //     SMS Subscription
    //     Log and Undeliverable
      TrackingEventHelper.retrieveEvents(client, ETBounceEvent.class, new ETFilter());
      TrackingEventHelper.retrieveEvents(client, ETClickEvent.class, new ETFilter());
      TrackingEventHelper.retrieveEvents(client, ETOpenEvent.class, new ETFilter());
      TrackingEventHelper.retrieveEvents(client, ETUnsubEvent.class, new ETFilter());
  }


}
