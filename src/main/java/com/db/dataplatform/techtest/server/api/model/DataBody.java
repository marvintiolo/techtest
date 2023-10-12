package com.db.dataplatform.techtest.server.api.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@JsonSerialize(as = DataBody.class)
@JsonDeserialize(as = DataBody.class)
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class DataBody {

    @NotNull
    private String dataBody;

    private String md5Checksum;

	public String getDataBody() {
		return dataBody;
	}

	public void setDataBody(String dataBody) {
		this.dataBody = dataBody;
	}

	public String getMd5Checksum() {
		return md5Checksum;
	}

	public void setMd5Checksum(String md5Checksum) {
		this.md5Checksum = md5Checksum;
	}
}
