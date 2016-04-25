package com.github.storm;

import java.io.Serializable;
import java.util.Date;

public class GitCommit implements Serializable {

	private static final long serialVersionUID = 1L;

	private String emailId;
	private String commitId;
	private Date commitDate;
	private String gitMessage;

	public String getEmailId() {
		return emailId;
	}

	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}

	public String getCommitId() {
		return commitId;
	}

	public void setCommitId(String commitId) {
		this.commitId = commitId;
	}

	public Date getCommitDate() {
		return commitDate;
	}

	public void setCommitDate(Date commitDate) {
		this.commitDate = commitDate;
	}

	public String getGitMessage() {
		return gitMessage;
	}

	public void setGitMessage(String gitMessage) {
		this.gitMessage = gitMessage;
	}

	public GitCommit(String emailId, String commitId, Date commitDate, String gitMessage) {
		super();
		this.emailId = emailId;
		this.commitId = commitId;
		this.commitDate = commitDate;
		this.gitMessage = gitMessage;
	}

	public GitCommit() {
		super();
	}

}
