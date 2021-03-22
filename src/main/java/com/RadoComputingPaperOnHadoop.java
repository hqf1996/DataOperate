package com;

import java.util.Calendar;
import java.util.Iterator;
import java.util.List;


public class RadoComputingPaperOnHadoop {
	//------参数列表------
	//论文总系数，为了百分制
	final double PAPER_RATIO=100/68.48513942;
	final double PAPER_NOWYEARS_RATIO=100/34.22807115;
	//论文等级参数
	final double PAPER_LEVEL_FIRSTGRADE=0.7;
	final double PAPER_LEVEL_CORE=0.2;
	final double PAPER_LEVEL_OTHER=0.1;

	//第几作者计算底数
	final double PAPER_AUTHER_BASE=0.85;
	//计算时间表达式前的系数
	//e的-PAPER_SPANYEAR_RATIO*spanyear次
	final double PAPER_SPANYEAR_RATIO=0.1;
	final double PAPER_NOW_YEARS=3;//三年之内

	/**
	 *
	 * @param expertID 专家ID
	 * @param paperList
	 * 		一个专家对应的所有论文的集合
	 * @return
	 */
	public double[] PaperComputing(String expertID, List<List<String>> paperList) {

    	double sumPaperValue=0;
    	double nowYearSumPaperValue=0;

    	try {

    		for (List<String> thisPaperInfo : paperList) {
				String leaderString = thisPaperInfo.get(0);
				String memberString = thisPaperInfo.get(1);
				String journalnameString = thisPaperInfo.get(2);
				String paperYearString = thisPaperInfo.get(3);

/*				System.out.println(leaderString);
				System.out.println(memberString);
				System.out.println(journalnameString);
				System.out.println(paperYearString);*/

	        	//判断第几作者
	        	int autherNo = Integer.parseInt(thisPaperInfo.get(4));
	        	//System.out.println(autherNo);
	        	//判断项目级别
	        	double PaperLevel=PAPER_LEVEL_OTHER;
	        	if(journalnameString==null)
	        		PaperLevel=PAPER_LEVEL_OTHER;
	        	else{
	        		if(journalnameString.indexOf("学报")!=-1||journalnameString.indexOf("学报")!=-1)
	            		PaperLevel=PAPER_LEVEL_FIRSTGRADE;

	            	if(journalnameString.indexOf("大学")!=-1||journalnameString.indexOf("学院")!=-1)
	            		PaperLevel=PAPER_LEVEL_CORE;
	        	}

	        	//计算时间间隔
	        	int spanYears=8;
	        	Calendar c = Calendar.getInstance();//可以对每个时间域单独修改
	        	int thisYear = c.get(Calendar.YEAR);


	        	if(paperYearString!=null&&!paperYearString.equals("")){
	        		try {
	        			paperYearString=paperYearString.replaceAll(" ", " ").trim().substring(0,4);
	            		spanYears=thisYear-Integer.valueOf(paperYearString);
					} catch (Exception e) {
						spanYears=25;
					}
	        	}
	        	if(spanYears>25||spanYears<0)//保证数据有效性，超出该区间无意义
	        		spanYears=25;
	        	//依次计算当前项目的加权
	        	double thisPapervalue=0;
	        	//thisPaperalue=PAPER_RATIO*PaperLevel*(Math.pow(totalFund,PAPER_TOTALFUND_RATIO))*Math.pow(Math.E, (-1)*PAPER_SPANYEAR_RATIO*spanYears)*Math.pow(PAPER_AUTHER_BASE,autherNo);
	        	thisPapervalue=PAPER_RATIO*PaperLevel*Math.pow(Math.E, (-1)*PAPER_SPANYEAR_RATIO*spanYears)*Math.pow(PAPER_AUTHER_BASE,autherNo);

	        	sumPaperValue=sumPaperValue+thisPapervalue;

	        	if(spanYears<=PAPER_NOW_YEARS)
	        		nowYearSumPaperValue=nowYearSumPaperValue+thisPapervalue;

			}
	    } catch (Exception e) {
			e.printStackTrace();
		}

        double result[]=new double[2];
    	result[0]=sumPaperValue;
    	result[1]=nowYearSumPaperValue*PAPER_NOWYEARS_RATIO;
    	return result;
	}

	private int getAutherNoForPaper(String member,String expertName){
		//该函数用于从成员字段中判断当前专家处于第几作者，默认member是以第一作者出现的
		//member中每个成员以（或者，符号隔开。
		int index=member.indexOf(expertName);
		String subString=member.substring(0, index);

		int count =0, start =0;
		if(subString.indexOf(";")==-1)
	        count=1;
		else
			 while((start=subString.indexOf(";",start))>=0){
		            start += ";".length();
		            count ++;
		        }
		return count+1;
	}


	public void PaperComputingAll(String expertID, List<List<String>> paperList) throws Exception {

		try {
			RadoComputingPaperOnHadoop radoPaper = new RadoComputingPaperOnHadoop();
			double paperResult[]=radoPaper.PaperComputing(expertID, paperList);
			for (int j = 0;j < paperResult.length; j++)
				System.out.println(paperResult[j]);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
