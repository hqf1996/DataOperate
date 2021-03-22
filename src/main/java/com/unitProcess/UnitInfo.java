package com.unitProcess;

public class UnitInfo {
	private String typeCode;
	private String unit;
	private String provinceCode;
	private String city;
	private String rankCode;
	private String unitCode;
	public UnitInfo() {
		// TODO Auto-generated constructor stub
		typeCode = "null";
		unit = "null";
		provinceCode = "99";
		city = "null";
		rankCode = "999";
		unitCode = "99999999";
	}
	
	public String getTypeCode() {
		return typeCode;
	}
	public void setTypeCode(String typeCode) {
		this.typeCode = typeCode;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public String getProvinceCode() {
		return provinceCode;
	}
	public void setProvinceCode(String provinceCode) {
		this.provinceCode = provinceCode;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getRankCode() {
		return rankCode;
	}
	public void setRankCode(String rankCode) {
		this.rankCode = rankCode;
	}
	public String getUnitCode() {
		return unitCode;
	}
	public void setUnitCode(String unitCode) {
		this.unitCode = unitCode;
	}
	public String join(String s){
		return unit + s + typeCode + s + provinceCode + s + unitCode + s + rankCode + s + city;
	}
	@Override
	public String toString() {
		return join("\t");
	}
}
