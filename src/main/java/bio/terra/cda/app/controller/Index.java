package bio.terra.cda.app.controller;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.net.URI;

@Controller
@ResponseBody
@RequestMapping("/")
public class Index {
    @GetMapping("/")
    public ResponseEntity<Object>home(RedirectAttributes attributes){
        return ResponseEntity.status(HttpStatus.FOUND).location(URI.create("/api/swagger-ui.html")).build();
    }

}

