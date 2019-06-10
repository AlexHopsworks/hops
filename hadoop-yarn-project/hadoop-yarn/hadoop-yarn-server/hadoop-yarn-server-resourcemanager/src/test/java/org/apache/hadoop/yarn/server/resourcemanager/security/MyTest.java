package org.apache.hadoop.yarn.server.resourcemanager.security;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringEscapeUtils;
import org.junit.Test;

public class MyTest {
  @Test
  public void dateParseTest() {
    GsonBuilder parserBuilder = new GsonBuilder();
    parserBuilder.setFieldNamingPolicy(FieldNamingPolicy.IDENTITY);
    parserBuilder.setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    Gson jsonParser = parserBuilder.create();
    String d = "{\"expLeeway\":300,\"expiresAt\":\"2019-07-12T12:42:02.299Z\",\"nbf\":\"2019-07-12T12:12:02.299Z\",\"token\":\"eyJraWQiOiI0NCIsInR5cCI6IkpXVCIsImFsZyI6IkhTNTEyIn0.eyJhdWQiOiJqb2IiLCJzdWIiOiJtZWIxMDAwMCIsIm5iZiI6MTU2MjkzMzUyMiwicmVuZXdhYmxlIjpmYWxzZSwiZXhwTGVld2F5IjozMDAsInJvbGVzIjpbIkhPUFNfVVNFUiJdLCJpc3MiOiJob3Bzd29ya3NAbG9naWNhbGNsb2Nrcy5jb20iLCJleHAiOjE1NjI5MzUzMjIsImlhdCI6MTU2MjkzMzUyMiwianRpIjoiNDI1NDIyZjgtYjdhYi00MDA2LWI2NmUtYzY0NzIyOTMxYTU4In0.IVk0nQ-ecn8eB8gjW_RhfBr1SLz2Ul5k_s2554JT5Bcwl16zuRj0ZaJ2lodvgfomzcBMwloGFvjAULQZrfiM5A\"}";
    HopsworksRMAppSecurityActions.JWTDTO jwtResponse
      = jsonParser.fromJson(d, HopsworksRMAppSecurityActions.JWTDTO.class);
    System.out.println(d);
    System.out.println(StringEscapeUtils.escapeJava(d));
    System.out.println(jwtResponse.getNbf().toString());
    System.out.println(jwtResponse.getExpiresAt().toString());
  }
}
