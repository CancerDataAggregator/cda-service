package bio.terra.cda.app.controller;

import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;

public class HeaderUtils {

    public static HttpHeaders getNoCacheResponseHeader(){
        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setCacheControl(CacheControl.noStore());
        responseHeaders.setPragma("no-cache");
        return responseHeaders;
    }

}
