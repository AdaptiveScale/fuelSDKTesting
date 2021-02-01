package com.cirus.io;

import com.exacttarget.fuelsdk.ETClient;
import com.exacttarget.fuelsdk.ETDataExtensionRow;
import com.exacttarget.fuelsdk.ETExpression;
import com.exacttarget.fuelsdk.ETFilter;
import com.exacttarget.fuelsdk.ETResponse;
import com.exacttarget.fuelsdk.ETSdkException;
import com.google.common.collect.Lists;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Hello world!
 */
public class App {
  private static ETClient client = null;
  private static int debugIndex=0;

  public static void main(String[] args) {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.ERROR);
    String id = "";



      long startTime = System.currentTimeMillis();
      try {

      client = new ETClient("/fuelsdk.properties");

        ETExpression etExpression= new ETExpression();
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

//            ETResponse<ETDataExtensionRow> response = ETDataExtension.select(client, "key="+id);
//      ETResponse<ETDataExtensionRow> response = ExtETDataExtensionRow.select(client, "key=" + id);
//            ETResponse<ETDataExtensionRow> response = ETDataExtension.select(client, "key=" + id,(Integer) null, (Integer) null, "");
//
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
      System.out.println("Total Time " +(endTime-startTime)/1000);
            /*
            assertNull(result.getRequestId());
            assertNull(result.getStatus());
            assertNull(result.getResponseCode());
            assertNull(result.getResponseMessage());
            assertNull(result.getErrorCode());
            assertEquals(id, result.getObjectId());
            ETDataExtension de = result.getObject();
            assertEquals(id, de.getId());
            assertEquals("test1", de.getKey());
            assertEquals("test1", de.getName());
            assertEquals("", de.getDescription());
            System.out.println("Hello World!");

             */
    } catch (ETSdkException ex) {
      System.out.println(ex);
    }

  }



  private static void console(List<ETDataExtensionRow> rows) {
//    for (ETDataExtensionRow row : rows) {
//      System.out.print("Row ID " + debugIndex++ + " ");
//      for (String colName : row.getColumnNames()) {
//        System.out.print(colName + " " + row.getColumn(colName) + " ** ");
//      }
//      System.out.println();
//    }
  }
}
