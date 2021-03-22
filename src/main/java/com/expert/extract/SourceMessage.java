package com.expert.extract;

public interface SourceMessage {
	final String PAPERFIRSTUNIT = "4";//论文第一作者有单位
	final String PAPERNOFIRST = "41";//论文其他作者
	final String PAPERFIRSTNOUNIT = "44";//论文第一作者没有单位
	final String PROJECTFIRST = "2";//项目第一发明人
	final String PROJECTNOFIRST = "21";//项目其他发明人
	final String PATENTFIRST = "3";//专利第一发明人
	final String PATENTNOFIRST = "31";//专利其他发明人
	final String PAPER = "4";
	final String PATENT = "3";
	final String PROJECT = "2";
}
