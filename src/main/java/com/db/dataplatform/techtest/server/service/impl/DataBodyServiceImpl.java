package com.db.dataplatform.techtest.server.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.persistence.repository.DataHeaderRepository;
import com.db.dataplatform.techtest.server.persistence.repository.DataStoreRepository;
import com.db.dataplatform.techtest.server.service.DataBodyService;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DataBodyServiceImpl implements DataBodyService {

	@Autowired
    DataStoreRepository dataStoreRepository;
	@Autowired
	DataHeaderRepository dataHeaderRepository;

    @Override
    public void saveDataBody(DataBodyEntity dataBody) {
        dataStoreRepository.save(dataBody);
    }

    @Override
    public List<DataBodyEntity> getDataByBlockType(BlockTypeEnum blockType) {
    	List<DataBodyEntity> dataByBlockTypeList = new ArrayList<DataBodyEntity>();
    	List<DataBodyEntity> dataBodyEntities = dataStoreRepository.findAll();
    	for (DataBodyEntity dataBodyEntity: dataBodyEntities) {
    		DataHeaderEntity dataHeaderEntity = dataHeaderRepository
    				.findById(dataBodyEntity.getDataHeaderEntity().getDataHeaderId()).get();
    		if (dataHeaderEntity.getBlocktype().equals(blockType)) {
    			dataByBlockTypeList.add(dataBodyEntity);
    		}
    	}
        return dataByBlockTypeList;
    }

    @Override
    public Optional<DataBodyEntity> getDataByBlockName(String blockName) {
    	List<DataBodyEntity> dataBodyEntities = dataStoreRepository.findAll();
    	long bodyEntityId = 0;
    	for (DataBodyEntity dataBodyEntity: dataBodyEntities) {
    		DataHeaderEntity dataHeaderEntity = dataHeaderRepository
    				.findById(dataBodyEntity.getDataHeaderEntity().getDataHeaderId()).get();
    		if (blockName.equals(dataHeaderEntity.getName())) {
    			bodyEntityId = dataBodyEntity.getDataStoreId();
    		}
    	}
        return dataStoreRepository.findById(bodyEntityId);
    }
}
