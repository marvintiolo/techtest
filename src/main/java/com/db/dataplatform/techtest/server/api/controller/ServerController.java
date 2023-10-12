package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.Valid;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope);
        
        if (checksumPass) {
        	log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());
            return ResponseEntity.ok(checksumPass);
        } else {
        	log.info("Checksum did not match.");
        	return ResponseEntity.notFound().build();
        } 
    }
    
    @GetMapping("/getAllData/{blockType}")
    public ResponseEntity<List<DataEnvelope>> getAllData(@PathVariable String blockType) {
    	return new ResponseEntity<List<DataEnvelope>>(server.getAllData(blockType), HttpStatus.OK);
    }

    @PutMapping("/updateData/{blockName}/{newBlockType}")
    public ResponseEntity<Boolean> updateData(@PathVariable String blockName, @PathVariable String newBlockType) {
    	boolean isUpdated = server.updateData(blockName, newBlockType);
    	return ResponseEntity.ok(isUpdated);
    }
}
