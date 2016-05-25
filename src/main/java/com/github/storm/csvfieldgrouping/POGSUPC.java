package com.github.storm.csvfieldgrouping;

public class POGSUPC {

	private String pog;
	private String supc;

	public POGSUPC() {

	}

	@Override
	public String toString() {
		return "POGSUPC [pog=" + pog + ", supc=" + supc + "]";
	}

	public POGSUPC(String pog, String supc) {
		super();
		this.pog = pog;
		this.supc = supc;
	}

	public String getPog() {
		return pog;
	}

	public void setPog(String pog) {
		this.pog = pog;
	}

	public String getSupc() {
		return supc;
	}

	public void setSupc(String supc) {
		this.supc = supc;
	}

}
