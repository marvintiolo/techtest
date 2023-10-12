package com.db.dataplatform.techtest.server.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonSerialize(as = DataEnvelope.class)
@JsonDeserialize(as = DataEnvelope.class)
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class DataEnvelope {

    @NotNull
    @Valid
    private DataHeader dataHeader;

    @NotNull
    private DataBody dataBody;

	public DataHeader getDataHeader() {
		return dataHeader;
	}

	public void setDataHeader(DataHeader dataHeader) {
		this.dataHeader = dataHeader;
	}

	public DataBody getDataBody() {
		return dataBody;
	}

	public void setDataBody(DataBody dataBody) {
		this.dataBody = dataBody;
	}
}
