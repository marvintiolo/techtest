package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.service.DataHeaderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {
	
	@Autowired
    DataBodyService dataBodyServiceImpl;
	@Autowired
	DataHeaderService dataHeaderServiceImpl;
	@Autowired
    ModelMapper modelMapper;
    
    private static final String CLIENT_MD5_CHECKSUM = "techtest";
    private static final String HADOOP_API = "http://localhost:8090/hadoopserver/pushbigdata";

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {
    	boolean isMd5Checksum = false;
    	
    	MessageDigest md5 = MessageDigest.getInstance("MD5");
    	md5.update(CLIENT_MD5_CHECKSUM.getBytes());
    	byte[] digest = md5.digest();
    	StringBuilder result = new StringBuilder();
        for (byte b : digest) {
            result.append(String.format("%02x", b));
        }
        String clientCheckSum = result.toString();
    	
    	DataBody dataBody = envelope.getDataBody();
    	String envelopChecksum = dataBody.getMd5Checksum();
    	
    	if (clientCheckSum.equals(envelopChecksum)) {
    		isMd5Checksum = true;
    		// Save to persistence.
            persist(envelope);
            pushDataToHadoopApi(envelope);
            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
    	}
        return isMd5Checksum;
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }
    
    private void pushDataToHadoopApi(DataEnvelope dataEnvelope) {
    	try {
			HttpClient httpClient = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost(HADOOP_API);

			ObjectMapper dataEnvelopMapper = new ObjectMapper();
			String jsonString = dataEnvelopMapper.writeValueAsString(dataEnvelope);

			StringEntity entity = new StringEntity(jsonString);
			httpPost.setEntity(entity);
			httpPost.setHeader("Content-Type", "application/json");

			HttpResponse response = httpClient.execute(httpPost);
			log.info("Response Code: " + response.getStatusLine().getStatusCode());
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

	@Override
	public List<DataEnvelope> getAllData(String blockType) {
		List<DataEnvelope> dataEnvelopList = new ArrayList<DataEnvelope>();
		List<DataBodyEntity> dataBodyList = dataBodyServiceImpl.getDataByBlockType(BlockTypeEnum.valueOf(blockType));
		for (DataBodyEntity dataBodyEntity: dataBodyList) {
			DataEnvelope dataEnvelope = new DataEnvelope();
			DataHeader dataHeader = modelMapper.map(dataBodyEntity, DataHeader.class);
			DataBody dataBody = modelMapper.map(dataBodyEntity, DataBody.class);
			dataEnvelope.setDataHeader(dataHeader);
			dataEnvelope.setDataBody(dataBody);
			dataEnvelopList.add(dataEnvelope);
		}
		return dataEnvelopList;
	}

	@Override
	public boolean updateData(String blockName, String newBlockType) {
		DataBodyEntity dataBodyEntity = dataBodyServiceImpl.getDataByBlockName(blockName).get();
		DataHeaderEntity dataHeaderEntity = dataBodyEntity.getDataHeaderEntity();
		dataHeaderEntity.setBlocktype(BlockTypeEnum.valueOf(newBlockType));
		dataHeaderServiceImpl.saveHeader(dataHeaderEntity);
		return true;
	}
}
