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
import com.exacttarget.fuelsdk.ETSoapObject;
import com.exacttarget.fuelsdk.ETUnsubEvent;
import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest {
  private static ETClient client = null;
  private static String id = "";
  private static int debugIndex;
  private static ArrayList<Class<? extends ETSoapObject>> eventTypes = Lists.newArrayList(ETOpenEvent.class,
                                                                                          ETBounceEvent.class,
                                                                                          ETClickEvent.class,
                                                                                          ETUnsubEvent.class);


  @Before
  public void setup() throws ETSdkException {
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ALL);

    client = new ETClient(    System.getenv("fuelsdk_prop"));
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



  @Test
  public void testTrackingEvent() {
    allTrackingEvents(new ETFilter());
  }

  @Test
  public void testOpenTrackingEventWithFilter() throws ETSdkException {
    ETFilter etFilter = ETFilter.parse("eventDate > 2010-01-01");
    allTrackingEvents(etFilter);
  }


  /**
   *  Simulate continue request id for open events
   *  Currently in staging data set there are not enough data to test this scenario - max is 953
   *  Simulate pagination by requesting 10 entities for page
   *
   * @throws ETSdkException
   */
  @Test
  public void testBatchSizeForOpenEvents() throws ETSdkException {

    long startTime = System.currentTimeMillis();
    try {
      ETResponse<ETOpenEvent> response = CustomETOpenEvent.customRetrieve(client, null, new ETFilter(), null, ETOpenEvent.class);
      System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
      System.out.println(response.getTotalCount() + " " + response.getPageSize());
      List<ETOpenEvent> rows = response.getObjects();
      printEvents(rows);


      while (response.getResponseMessage().equals("MoreDataAvailable")) {
        System.out.println("----------------- Next -------------");
        response = CustomETOpenEvent.customRetrieve(client, null, new ETFilter(), response.getRequestId(), ETOpenEvent.class);
        System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
        System.out.println(response.getTotalCount() + " " + response.getPageSize());
        printEvents(response.getObjects());
      }

      long endTime = System.currentTimeMillis();
      System.out.println("Total Time " + (endTime - startTime) / 1000);

    } catch (ETSdkException ex) {
      System.out.println(ex);
    } finally {
      debugIndex = 0;
    }
  }

  /**
   *  Simulate continue request id for open events
   *  Currently in staging data set there are not enough data to test this scenario - max is 953
   *  Simulate pagination by requesting 10 entities for page
   *
   * @throws ETSdkException
   */
  @Test
  public void testBatchSizeForOpenEventsWithFilter() throws ETSdkException {
    ETFilter etFilter = ETFilter.parse("eventDate > 2020-12-01");
    long startTime = System.currentTimeMillis();
    try {
      ETResponse<ETOpenEvent> response = CustomETOpenEvent.customRetrieve(client, null,etFilter, null, ETOpenEvent.class);
      System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
      System.out.println(response.getTotalCount() + " " + response.getPageSize());
      List<ETOpenEvent> rows = response.getObjects();
      printEvents(rows);


      while (response.getResponseMessage().equals("MoreDataAvailable")) {
        System.out.println("----------------- Next -------------");
        response = CustomETOpenEvent.customRetrieve(client, null, etFilter, response.getRequestId(), ETOpenEvent.class);
        System.out.println(response.getRequestId() + " " + response.getResponseCode() + " " + response.getResponseMessage());
        System.out.println(response.getTotalCount() + " " + response.getPageSize());
        printEvents(response.getObjects());
      }

      long endTime = System.currentTimeMillis();
      System.out.println("Total Time " + (endTime - startTime) / 1000);

    } catch (ETSdkException ex) {
      System.out.println(ex);
    } finally {
      debugIndex = 0;
    }
  }

  private void allTrackingEvents(ETFilter filter) {
    for (Class<? extends ETSoapObject > eventType : eventTypes) {
      TrackingEventHelper.retrieveEvents(client, eventType, filter);
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

  private static void printEvents(List<ETOpenEvent> rows) {
    for (ETOpenEvent row : rows) {
      System.out.print("Row ID " + debugIndex++ + " ");
      System.out.println(row.toString());
      System.out.println();
    }
  }

}
