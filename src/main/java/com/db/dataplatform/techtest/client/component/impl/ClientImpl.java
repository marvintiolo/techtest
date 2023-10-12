package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

	public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
	public static final UriTemplate URI_GETDATA = new UriTemplate(
			"http://localhost:8090/dataserver/getAllData/{blockType}");
	public static final UriTemplate URI_PATCHDATA = new UriTemplate(
			"http://localhost:8090/dataserver/update/{blockName}/{newBlockType}");

	@Override
	public void pushData(DataEnvelope dataEnvelope) {
		log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
		try {
			HttpClient httpClient = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost(URI_PUSHDATA);

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
	public List<DataEnvelope> getData(String blockType) {
		log.info("Query for data with header block type {}", blockType);
		List<DataEnvelope> dataEnvelopeList = new ArrayList<DataEnvelope>();
		try {
			HttpClient httpClient = HttpClients.createDefault();
			UriComponentsBuilder builder = UriComponentsBuilder
					.fromUriString(URI_GETDATA.toString());
			String getUri = builder.buildAndExpand(blockType).toUriString();

			HttpGet httpGet = new HttpGet(getUri);

			HttpResponse response = httpClient.execute(httpGet);
			log.info("Response Code: " + response.getStatusLine().getStatusCode());

			HttpEntity entity = response.getEntity();
			String responseString = EntityUtils.toString(entity);
			ObjectMapper dataEnvelopMapper = new ObjectMapper();

			dataEnvelopeList = dataEnvelopMapper.readValue(responseString, new TypeReference<List<DataEnvelope>>() {
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dataEnvelopeList;
	}

	@Override
	public boolean updateData(String blockName, String newBlockType) {
		log.info("Updating blocktype to {} for block with name {}", newBlockType, blockName);
		HttpClient httpClient = HttpClients.createDefault();
		UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(URI_PATCHDATA.toString())
				.path(blockName).path(newBlockType);
		String putUri = builder.build().toUriString();
		
		HttpPut httpPut = new HttpPut(putUri);

		HttpResponse response = httpClient.execute(httpPut);
		log.info("Response Code: " + response.getStatusLine().getStatusCode());
		return true;
	}

}
