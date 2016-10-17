package weather.nmc.pop.fc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.LatLonRect;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class Grib2ParserNgThreads implements Runnable {
	
	/**
	 * @author Robin
	 * 
	 * nmc多线程测试，采用多线程计算,然后写文件
	 * 
	 * 
	 */
	
	private static float[] lat = null;  //grib2文件纬度
	
	private static float[] lon = null;   //grib2文件经度
	
	private static float[] latng = null;//插值后的纬度
	
	private static float[] lonng = null;//插值后的经度
	
	private static double[] time = null;//时间
	
	private static float[][][] source = null;//grib2文件的数据
	
	private static float[][][][] source2 = null;   //解析风力风向文件时由于有两个要素，所以需要用到这个source2
	
	private static float[][][][] source3 = null; //相对湿度为4维数组，解析相对湿度的时候要用这个source3,还有风力风向
	
	private static int lonRange = 0;//插值后的纬度范围
	
	private static int latRange = 0;//插值后的经度范围
	
	private static int timeRange = 0;//时间范围
	
	private static int sourceLatRange = 0;
	
	private static int sourceLonRange = 0;
	
	private int ratio = 5;//插值精度
	
	private float startLat = 0.0F;
	
	private float startLon = 70.0F;
			
	private float endLat = 60.0F;
	
	private float endLon = 140.0F;	
	
	private float latLonStep = 0.01F;
	
	private static float[][][] tempCorrection = null;
	
//	private static int realTime,realTimeCount;
	
	private static int ThreadNum = 10;//线程数
	private static byte[] ee,ee_12h;        
	private static short[]gg,gg_12h;
	private static short[]wsd,wdr,wsd_12h,wdr_12h;
	
	private static Set<Integer> set = new HashSet<Integer>();
	private static String FcType;
	private static int timeNum;
	private static ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<Integer>();
	private static ConcurrentLinkedQueue<Integer> queue_1h = new ConcurrentLinkedQueue<Integer>();
	private static boolean flag = true;
	static Logger logger = LogManager.getLogger(Grib2ParserNgThreads.class.getName());
	@Override
	public void run() {
		Grib2ParserNgThreads gpt = new Grib2ParserNgThreads();
		try {
			if(flag){
				if("wind".equals(FcType)){
					gpt.WindDataCal();
				}else{
					gpt.DataCal();
				}
			}else{
				gpt.interpolation();
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		
		Grib2ParserNgThreads gpt = new Grib2ParserNgThreads();
		FcType = args[0];
		
//		String dateS = args[1];
//		FcType = "wind";
//		String dateS = "2016101008";
		SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
		try {
			Date date;
			if(args.length>1){
				date = sdfh.parse(args[1]);
			}else{
				date = new Date();
			}
//			Date date = sdfh.parse(dateS);
//			Date date = new Date();
			DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String time1=format.format(date);
			System.out.println("开始时间："+time1);
			logger.error(FcType+"-->开始时间："+time1);
			long begin = System.currentTimeMillis(); 
			
			InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
		 	Properties properties = new Properties();
			properties.load(inputStream);
			String fcTime = properties.getProperty("fcTime");  //获取预报时次
			String[] fcTimeArray = fcTime.split(","); //预报时次数组
			String fileTime = properties.getProperty("fileTime");
			String[] fileTimeArray = fileTime.split(",");   //
			String pro = properties.getProperty(FcType);     //获取要素类型名称，并且用要素名称作为后来生成的nc文件的名称
			String filePath = properties.getProperty("filePath");
			String element = null;
			String elementU = null;
			String elementV = null;
			String fileNameHead = null;
			String timeStep = null;
			String elementName = null;
			String corFilePath = null;
			if("wind".equals(FcType)){
				elementU = pro.split(",")[1];//grib2文件要素名称
				elementV = pro.split(",")[2];
				fileNameHead = pro.split(",")[0];//文件名头部分
				timeStep = pro.split(",")[3];   //文件名时间步长
				elementName = pro.split(",")[4];    //写nc文件的要素名称
			}else{
				fileNameHead = pro.split(",")[0];//文件名头部分
				element = pro.split(",")[1].trim();//grib2文件要素名称
				timeStep = pro.split(",")[2].trim();   //文件名时间步长
				elementName = pro.split(",")[3].trim();    //写nc文件的要素名称
				if("temp".equals(FcType)){
					corFilePath = pro.split(",")[4].trim();
				}
			}
			String outPutPath = properties.getProperty("outPutPath");//输出目标路径
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String fileDay = sdf.format(date);
			int hour = date.getHours();
			String fileDate = null;
			if(hour>=12){      //判断，如果当前时间为12点以前，则读取08时次的grib2文件，否则读取20时次的grib2文件
				fileDate = fileDay+"2000"+timeStep;
			}else{
				fileDate = fileDay+"0800"+timeStep;
			}		
			if("wind".equals(FcType)){
				gpt.ReadGrib2WindFile(date,elementU,elementV,fileNameHead,timeStep,elementName,outPutPath,filePath);
//(Date date,String elementU,String elementV,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath)				
			}else{
				gpt.ReadGrib2File(date,element,fileNameHead,timeStep,elementName,outPutPath,filePath,corFilePath);
			}
//(Date date,String fcTime,String element,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath,String corFilePath)			
			for(timeNum = 0;timeNum<fcTimeArray.length;timeNum++){
				if("wind".equals(FcType)){
					wsd = new short[Integer.valueOf(fileTimeArray[0]) * (latRange) * (lonRange)];
					wdr = new short[Integer.valueOf(fileTimeArray[0]) * (latRange) * (lonRange)];
					//new12小时的文件，后面加个判断，要是第二次循环进来就不new了，接着上次的数组接着写。
					
					
//					wsd_12h = new short[Integer.valueOf(fileTimeArray[1]) * (latRange) * (lonRange)];
//					wdr_12h = new short[Integer.valueOf(fileTimeArray[1]) * (latRange) * (lonRange)];
				}else if("rh".equals(FcType) || "cloud".equals(FcType)){
					ee = new byte[Integer.valueOf(fileTimeArray[0]) * (latRange) * (lonRange)];
//					ee_12h = new byte[Integer.valueOf(fileTimeArray[1]) * (latRange) * (lonRange)];
				}else{
					gg = new short[Integer.valueOf(fileTimeArray[0]) * (latRange) * (lonRange)];
//					gg_12h = new short[Integer.valueOf(fileTimeArray[1]) * (latRange) * (lonRange)];
				}
//	        	timeData = new double[realTime];
				queue.clear();
				set.clear();
				for(int j = (timeNum == 0 ? 0 : 32);j < (timeNum == 0 ? 32 : 80);j++){ //将时次放入队列中   ,没有办法，只能这样写死了，以后需求变了就改这里吧，将80个时次文件分为前一个文件32时次，后一个文件48时次，其中前一个文件的前24小时是逐小时的，
					queue.add(j);
				}
				ExecutorService exe = Executors.newFixedThreadPool(ThreadNum);
				for (int i = 0; i < ThreadNum; i++){
					exe.execute(gpt);                                              //第一次启动线程，
		        }
				exe.shutdown();
				while (true) {
		            if (exe.isTerminated()) {
		            	flag = false;
		                System.out.println("线程运行结束");
		                
		                if(timeNum == 0){    //如果是第一个文件,为了获取前24小时逐小时的值，要用拉格朗日插值法或者别的插值方法，且写文件只用写逐小时的第一个文件
		                	ExecutorService exe_1h = Executors.newFixedThreadPool(ThreadNum);   //从新开线程计算,反演数据，真的是好麻烦啊
			            	for (int i = 0; i < ThreadNum; i++){
			            		exe_1h.execute(gpt);
					        }
			            	exe_1h.shutdown();
							while (true) {
								if (exe_1h.isTerminated()) {
									flag  = true;
									if("wind".equals(FcType)){
										gpt.WriteWindNcFile(outPutPath,elementName,fileDate,Integer.valueOf(fileTimeArray[0]));
									}else{
										gpt.WriteNcFile(outPutPath,elementName,element,fileDate,Integer.valueOf(fileTimeArray[0]));
									}
									break;
								}
							}
		                }else{//如果为第二次，则既要写逐小时文件，又要写逐12小时文件。
		                	if("wind".equals(FcType)){
								gpt.WriteWindNcFile(outPutPath,elementName,fileDate,Integer.valueOf(fileTimeArray[0]));
							}else{
								gpt.WriteNcFile(outPutPath,elementName,element,fileDate,Integer.valueOf(fileTimeArray[0]));
							}
		                }
		                System.gc();   //建议回收垃圾，只是建议，具体什么时候回收垃圾要看jvm的心情
		                Thread.sleep(500);
		                break;
		            }
		        }
			}
			long end = System.currentTimeMillis();
			Date date1=new Date();
			String time2=format.format(date1);
			System.out.println("结束时间："+time2);
			System.out.println("运行时间："+(end-begin)+"ms");
			logger.error(FcType + "-->结束时间："+time2);
			logger.error(FcType + "-->运行时间："+(end-begin)+"ms");
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
	}
	
	public void interpolation()throws Exception{                //插值，将逐3小时前24小时的数据插值成逐小时的数据
		while(!queue_1h.isEmpty()){
			int time = queue_1h.poll();
			int before_time = time % 3 == 1 ? time - 1 : time - 2;
			int after_time = time % 3 == 2 ? time + 1 : time + 2;
//			int other_time = time % 3 == 1 ? time + 1 : time -1;  //与其相邻的非原始预报时次的点
			System.out.println(time+"-->"+before_time+"-->" + after_time);
			for(int j = 0; j<latRange;j++){
				for(int k =0;k<lonRange;k++){
					if(FcType.equals("wind")){//如果要素为风，则取临近时次的值
						short wind_u_data;
						short wind_v_data;
						if(time % 3 == 1){
							wind_u_data = wsd[before_time * latRange * lonRange + j * lonRange + k];
							wind_v_data = wsd[before_time * latRange * lonRange + j * lonRange + k];
						}else{
							wind_u_data = wsd[after_time * latRange * lonRange + j * lonRange + k];
							wind_v_data = wsd[after_time * latRange * lonRange + j * lonRange + k];
						}
						wsd[time * latRange * lonRange + j * lonRange + k] = wind_u_data;
						wdr[time * latRange * lonRange + j * lonRange + k] = wind_v_data;
					}else if(FcType.equals("r03")){  //如果要素为降水量，则取前一个3的整数时次的值 除以3
						short before_data = gg[before_time * latRange * lonRange + j * lonRange + k];
//						short avg_data = new BigDecimal(before_data).divide(new BigDecimal(3),0,BigDecimal.ROUND_HALF_UP).shortValue();
						gg[time * latRange * lonRange + j * lonRange + k] = before_data;
						
					}else if(FcType.equals("rh") || "cloud".equals(FcType)){ //相对湿度用拉格朗日插值   byte数组ee[]
						byte before_data = ee[before_time * latRange * lonRange + j * lonRange + k];
						byte after_data = ee[after_time * latRange * lonRange + j * lonRange + k];
						int X[] = new int[]{before_time,after_time};
						int Y[] = new int[]{before_data,after_data};
						int X0 = time;
						byte data = Lagrange_byte(X, Y, X0);
						ee[time * latRange * lonRange + j * lonRange + k] = data;
					}else{// 温度、云量用拉格朗日插值，返回数组为gg[]  short类型
						short before_data = gg[before_time * latRange * lonRange + j * lonRange + k];
						short after_data = gg[after_time * latRange * lonRange + j * lonRange + k];
						int X[] = new int[]{before_time,after_time};
						int Y[] = new int[]{before_data,after_data};
						int X0 = time;
						short data = Lagrange_short(X, Y, X0);
						gg[time * latRange * lonRange + j * lonRange + k] = data;
					}
				}
			}
		}
	}
	
	public void interpolation_12h()throws Exception{
		
		
	}
	
	public short Lagrange_short(int X[],int Y[],int X0){   //拉格朗日插值，返回short类型值
		int m=X.length;
        short Y0;
        short t = 0;
        for(int i2=0;i2<m;i2++){
        	double u=1;
        	for(int i3=0;i3<m;i3++){
        		 if(i2!=i3){
                     u=u*(X0-X[i3])/(X[i2]-X[i3]);
                 }
        	}
        	u=u*Y[i2];
        	short a = new BigDecimal(u).shortValue();
        	t= (short) (t + a);
        }
        Y0=t;
		return Y0;
	}
	
	public byte Lagrange_byte(int X[],int Y[],int X0){ //拉格朗日插值，返回byte类型值
		int m=X.length;
        byte Y0;
        byte t = 0;
        for(int i2=0;i2<m;i2++){
        	double u=1;
        	for(int i3=0;i3<m;i3++){
        		 if(i2!=i3){
                     u=u*(X0-X[i3])/(X[i2]-X[i3]);
                 }
        	}
        	u=u*Y[i2];
        	byte a = new BigDecimal(u).byteValue();
        	t= (byte) (t + a);
        }
        Y0=t;
		return Y0;
	}
	
	
	
	public void ReadGrib2File(Date date,String element,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath,String corFilePath)throws Exception{   //读nc文件，并写入内存
		/*InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		properties.load(inputStream);
		String pro = properties.getProperty(FcType);     //获取要素类型名称，并且用要素名称作为后来生成的nc文件的名称
		String fcTime = properties.getProperty("fcTime");  //获取预报时次
		String[] fcTimeArray = fcTime.split(","); //预报时次数组
		String element = pro.split(",")[1].trim();//grib2文件要素名称
		String fileNameHead = pro.split(",")[0].trim();//文件名头部分
		String timeStep = pro.split(",")[2].trim();   //文件名时间步长
		String elementName = pro.split(",")[3].trim();    //写nc文件的要素名称
		String outPutPath = properties.getProperty("outPutPath");//输出目标路径
		String filePath = properties.getProperty("filePath");//Grib文件路径
*/		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		String beginHours = null;
		
		if(hour>=12){      //判断，如果当前时间为18点以前，则读取08时次的grib2文件，否则读取20时次的grib2文件
			beginHours = "20";
			fileDate = fileDay+"2000"+timeStep;
		}else{
			beginHours = "08";
			fileDate = fileDay+"0800"+timeStep;
		}	
		
		//如果为温度预报，则需要先解析温度预报的订正文件
		
		if(FcType.indexOf("temp")>=0){
//			String corFilePath = pro.split(",")[4].trim();
			String tempFileName = corFilePath + element + "_" + fileDay + beginHours + ".nc" ;
			File file = new File(tempFileName);
			System.out.println("open correction file -->" + tempFileName);
			if(file.exists()){
				NetcdfFile CorrectionFile = null;   //温度订正nc文件
				Variable CorrectionV = null;
				CorrectionFile = NetcdfFile.open(tempFileName);
				System.out.println("file exist and open success");
				CorrectionV = CorrectionFile.findVariable(elementName);
				int[] shape = CorrectionV.getShape();
				String section = "0," + "0:" + String.valueOf(shape[1]-1) + "," + "0:" + String.valueOf(shape[2]-1);
//				System.out.println(section);
				Array data = CorrectionV.read(section);
				tempCorrection = (float[][][]) data.copyToNDJavaArray();  //放入内存
				System.out.println(tempCorrection[0].length);
				System.out.println(tempCorrection[0][0].length);
				CorrectionFile.close();   //关闭文件
			}else{
				System.out.println("correction file not exist");
			}
		}
/*********************************************解析原始GRIB2文件begin*******************************************************************/
		
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //文件名，先注释掉，业务化后再打开
		
//		String fileName = "Z_NWGD_C_BABJ_P_RFFC_SCMOC-ER03_201609120800_24003.GRB2";
		System.out.println("begin open grib2 file -->"+fileName);
		//初始经纬度
		File file = new File(fileName);
		if(!file.exists()){   //判断源文件是否存在,若存不存在，则直接复制上一个时次生成的nc文件
			CopyFile(outPutPath, elementName, element, date, timeStep);
			return;
		}
		
		GridDataset gds = ucar.nc2.dt.grid.GridDataset.open(fileName);
		
		GridDatatype grid = gds.findGridDatatype(element);
		GridDatatype subGrid = grid.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		
		lat = (float[])gds.getNetcdfDataset().findVariable("lat").read().copyTo1DJavaArray();//原始文件纬度
		
		sourceLatRange = subGrid.getDimension(subGrid.getYDimensionIndex()).getLength();
		
		LatLonRect latLonRect = gds.getBoundingBox();
		
		latLonStep = (float)(Math.round((latLonRect.getWidth())/(lat.length - 1)/ratio*100))/100;  //步长
		
		latRange = ratio * (sourceLatRange - 1) + 1;
		
		lon = (float[])gds.getNetcdfDataset().findVariable("lon").read().copyTo1DJavaArray();  
		
		sourceLonRange = subGrid.getDimension(subGrid.getXDimensionIndex()).getLength();
		
		lonRange = ratio * (sourceLonRange - 1) + 1;
		
		latng = new float[latRange];
		
		for(int i = 0; i<latRange;i++ ){
			latng[i] = (float) (Math.round((lat[0] + i*latLonStep)*100) / 100.00);//0.01F
		}
		
		lonng = new float[lonRange];
		for(int i = 0; i<lonRange;i++ ){
			lonng[i] = (float) (Math.round((lon[0] + i*latLonStep)*100) / 100.00);//0.01F
		}
//		System.out.println("latLonStep=>"+latLonStep);
		
		time = (double[])gds.getNetcdfDataset().findVariable("time").read().copyTo1DJavaArray();
		
		timeRange = time.length;
		//source = new float[time.length*lat.length*lon.length];
//		System.out.println("timeRage => "+timeRange+" latRange => "+ latRange + " lonRange => "+lonRange);
		
		//读取原始数据
		if("rh".equals(FcType)||"temp".equals(FcType)){
			source3 = (float[][][][])subGrid.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		}else{
			source = (float[][][])subGrid.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		}
		gds.close();
	}
	
	public void ReadGrib2WindFile(Date date,String elementU,String elementV,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath)throws Exception{
		
		/*InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		properties.load(inputStream);
		String pro = properties.getProperty(FcType);     //获取要素类型名称，并且用要素名称作为后来生成的nc文件的名称
		String elementU = pro.split(",")[1];//grib2文件要素名称
		String elementV = pro.split(",")[2];
		String fileNameHead = pro.split(",")[0];//文件名头部分
		String timeStep = pro.split(",")[3];   //文件名时间步长
		String elementName = pro.split(",")[4];    //写nc文件的要素名称
		String outPutPath = properties.getProperty("outPutPath");//输出目标路径
		String filePath = properties.getProperty("filePath");//Grib文件路径
*/		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		if(hour>=12){      //判断，如果当前时间为18点以前，则读取08时次的grib2文件，否则读取20时次的grib2文件
			fileDate = fileDay+"2000"+timeStep;
		}else{
			fileDate = fileDay+"0800"+timeStep;
		}
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //文件名，先注释掉，业务化后再打开
//		String fileName = "Z_NWGD_C_BABJ_20161009201616_P_RFFC_SCMOC-EDA10_201610100800_24003.GRB2";
		System.out.println("begin open grib2 file -->"+fileName);
		File file = new File(fileName);
		if(!file.exists()){   //判断源文件是否存在,若存不存在，则直接复制上一个时次生成的nc文件
			CopyFile(outPutPath, elementName, "wind", date, timeStep);
			return;
		}
		GridDataset gds = ucar.nc2.dt.grid.GridDataset.open(fileName);
		GridDatatype gridU = gds.findGridDatatype(elementU);
		GridDatatype gridV = gds.findGridDatatype(elementV);
		GridDatatype subGridU = gridU.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		GridDatatype subGridV = gridV.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		lat = (float[])gds.getNetcdfDataset().findVariable("lat").read().copyTo1DJavaArray();//原始文件纬度
		sourceLatRange = subGridU.getDimension(subGridU.getYDimensionIndex()).getLength();
		LatLonRect latLonRect = gds.getBoundingBox();
		latLonStep = (float)(Math.round((latLonRect.getWidth())/(lat.length - 1)/ratio*100))/100;  //步长
		latRange = ratio * (sourceLatRange - 1) + 1;
		lon = (float[])gds.getNetcdfDataset().findVariable("lon").read().copyTo1DJavaArray();  
		sourceLonRange = subGridU.getDimension(subGridU.getXDimensionIndex()).getLength();
		lonRange = ratio * (sourceLonRange - 1) + 1;
		latng = new float[latRange];
		for(int i = 0; i<latRange;i++ ){
			latng[i] = (float) (Math.round((lat[0] + i*latLonStep)*100) / 100.00);//0.01F
		}
		lonng = new float[lonRange];
		for(int i = 0; i<lonRange;i++ ){
			lonng[i] = (float) (Math.round((lon[0] + i*latLonStep)*100) / 100.00);//0.01F
		}
		time = (double[])gds.getNetcdfDataset().findVariable("time").read().copyTo1DJavaArray();
		timeRange = time.length;
		//读取原始数据
		source2 = (float[][][][])subGridU.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		source3 = (float[][][][])subGridV.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		gds.close();
	}
	
	public void DataCal() throws Exception{  //多线程计算
		int lat_min,lat_max,lon_min,lon_max;
		while(!queue.isEmpty()){                        //当队列不为空时，执行出列
			int time = queue.poll();
			if(timeNum == 0 & time < 8){             //添加逐小时待反演数据时次：1,2,4,5,7,8,10,11,13,14,16,17,19,20,22,23
				queue_1h.add(time * 3 + 1);
				queue_1h.add(time * 3 + 2);
			}
			
			int timeData = 0;  //~……~！
			if(timeNum == 0 & time <=8){    //第一次逐小时，前24小时
				timeData = time * 3;
			}else if(timeNum == 0 & time >8){ //第一次逐小时，24小时后
				timeData = time + 16;
			}else {
				timeData = time - 32;
			}
			
			set.add(time);
			System.out.println(time + "-->" + timeData);
			for(int j = 0; j<latRange;j++){
				if(j == latRange-1){
					lat_min = j / ratio -1;
					lat_max = lat_min+1;
				}else{
					lat_min = j / ratio;
					lat_max = lat_min+1;
				}
				float lat_1Km = latng[j];
				for(int k =0;k<lonRange;k++){
					if(k == lonRange-1){
						lon_min = k / ratio - 1;
						lon_max = lon_min+1;
					}else {
						lon_min = k / ratio;
						lon_max = lon_min+1;
					}
					float lon_1Km = lonng[k];
					float lat_5Km_min = (float) (Math.round(lat[lat_min]*1000)/1000.000);
					float lat_5Km_max = (float) (Math.round(lat[lat_max]*1000)/1000.000);
					float lon_5Km_min = (float) (Math.round(lon[lon_min]*1000)/1000.000);
					float lon_5Km_max = (float) (Math.round(lon[lon_max]*1000)/1000.000);
					
					if("rh".equals(FcType) || "cloud".equals(FcType)){
						byte data = DoubleLineInter3(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  //通过插值获取相对湿度的值
						ee[timeData * latRange * lonRange + j * lonRange + k] = data;
					}else if("r03".equals(FcType) & timeData < 24){
						short data = DoubleLineInter(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  //通过插值获取降水量的值
						short avg_data = new BigDecimal(data).divide(new BigDecimal(3),0,BigDecimal.ROUND_HALF_UP).shortValue();
						avg_data  = (avg_data < 1 & data > 0) ? 1 : avg_data;
						/*if(avg_data > 0){
							System.out.println("timeData-->" + timeData + "--data-->" + data + "--avg_data-->" + avg_data);
						}*/
						gg[timeData * latRange * lonRange + j * lonRange + k] = avg_data;
					}else{
						short data = DoubleLineInter(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  //通过插值获取降水量的值
						gg[timeData * latRange * lonRange + j * lonRange + k] = data;
					}
				}
			}
		}
	}
	
	public void WindDataCal() throws Exception{  //多线程计算风力风向
		int lat_min,lat_max,lon_min,lon_max;
		while(!queue.isEmpty()){
			int time = queue.poll();
			if(timeNum == 0 & time < 8){             //添加逐小时待反演数据时次：1,2,4,5,7,8,10,11,13,14,16,17,19,20,22,23
				queue_1h.add(time * 3 + 1);
				queue_1h.add(time * 3 + 2);
			}
			
			int timeData = 0;  //~……~！
			if(timeNum == 0 & time <=8){
				timeData = time * 3;
			}else if(timeNum == 0 & time >8){
				timeData = time + 16;
			}else {
				timeData = time - 32;
			}
			
			set.add(time);
			System.out.println(time);
			for(int j = 0; j<latRange-1;j++){
				if(j == latRange-1){
					lat_min = j / ratio -1;
					lat_max = lat_min+1;
				}else{
					lat_min = j / ratio;
					lat_max = lat_min+1;
				}
				float lat_1Km = latng[j];
				for(int k =0;k<lonRange;k++){
					if(k == lonRange-1){
						lon_min = k / ratio - 1;
						lon_max = lon_min+1;
					}else {
						lon_min = k / ratio;
						lon_max = lon_min+1;
					}
					float lon_1Km = lonng[k];
					float lat_5Km_min = (float) (Math.round(lat[lat_min]*1000)/1000.000);
					float lat_5Km_max = (float) (Math.round(lat[lat_max]*1000)/1000.000);
					float lon_5Km_min = (float) (Math.round(lon[lon_min]*1000)/1000.000);
					float lon_5Km_max = (float) (Math.round(lon[lon_max]*1000)/1000.000);
					
					float wind_u = DoubleLineInter2(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"u");  //通过插值获取u分量的值
					float wind_v = DoubleLineInter2(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"v");  //通过插值获取v分量的值
					wsd[timeData * latRange * lonRange + j * lonRange + k] = (short) wind_u;
					wdr[timeData * latRange * lonRange + j * lonRange + k] = (short) wind_v;
				}
			}
		}
	}
	
	public void WriteNcFile(String outPutPath,String elementName,String element,String fileDate,int realTime)throws Exception{
		String filename = outPutPath + elementName +"/" + element+ "_" + fileDate + "_" + timeNum + ".nc";
//		String filename = elementName + "_" + timeNum + ".nc";
		System.out.println("netcdf out put path-->"+filename);
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//这种类型写的文件访问速度最快
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
	    dataFile = NetcdfFileWriter.createNew(version, filename,chunker);
    	Dimension xDim = dataFile.addDimension(null, "lat", latRange);
        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
        Dimension zDim = dataFile.addDimension(null, "time", realTime);  //默认逐个小时文件的每个文件时次为48个时次
        List<Dimension> dims_Element = new ArrayList<Dimension>();
        List<Dimension> dims_Lat = new ArrayList<Dimension>();
        List<Dimension> dims_Lon = new ArrayList<Dimension>();
        List<Dimension> dims_Time = new ArrayList<Dimension>();
        dims_Lat.add(xDim);
        dims_Lon.add(yDim);
        dims_Time.add(zDim);
        dims_Element.add(zDim);
        dims_Element.add(xDim);
        dims_Element.add(yDim);	
        Variable dataV = null;
		if("rh".equals(FcType) || "cloud".equals(FcType)){    //相对湿度范围为0-100，用byte类型存储
			dataV = dataFile.addVariable(null, elementName, DataType.BYTE,dims_Element);
		}else{
			dataV = dataFile.addVariable(null, elementName, DataType.SHORT,dims_Element);
		}
        Variable latV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
        Variable lonV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
        Variable timeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
        lonV.addAttribute(new Attribute("units", "degrees_east"));
        latV.addAttribute(new Attribute("units", "degrees_north"));
        dataFile.create();
        if("rh".equals(FcType) || "cloud".equals(FcType)){
			Array dataArray  = Array.factory(DataType.BYTE, new int[]{realTime,latng.length,lonng.length},ee);
			dataFile.write(dataV, dataArray);
		}else{
			Array dataArray  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},gg);
			dataFile.write(dataV, dataArray);
		}
        double[] timeData = new double[realTime];
        int index =0;
        Iterator<Integer> it = set.iterator();
        while(it.hasNext()){
        	int time = it.next();
        	if(timeNum == 0 & time < 8){//如果是第一次写逐小时的文件，则需要补齐前24小时逐小时的时次
        		timeData[index] = (time + 1) * 3 - 2;
        		timeData[index + 1] = (time + 1) * 3 -1;
        		timeData[index + 2] = (time + 1) * 3;
        		index +=3;
        	}else{
        		timeData[index] = (time + 1) * 3;
            	index++;
        	}
        }
     
        Arrays.sort(timeData);
		Array timeArray = Array.factory(DataType.DOUBLE, new int[]{realTime},timeData);
		dataFile.write(timeV, timeArray);
		dataFile.write(latV, Array.factory(latng));
		dataFile.write(lonV, Array.factory(lonng));
		System.out.println("write netcdf success!!!");
		dataFile.close();
	}
	
	public void WriteWindNcFile(String outPutPath,String elementName,String fileDate,int realTime)throws Exception{ //写风力风向的nc文件
		
		String filename = outPutPath + elementName +"/" + elementName+ "_" + fileDate + "_" + timeNum + ".nc";
//		String filename = elementName + "_" + timeNum + ".nc";
		System.out.println("netcdf out put path-->"+filename);
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,9,false);
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
	    dataFile = NetcdfFileWriter.createNew(version, filename,chunker);
    	Dimension xDim = dataFile.addDimension(null, "lat", latRange);
        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
        Dimension zDim = dataFile.addDimension(null, "time", realTime);
        List<Dimension> dims_U = new ArrayList<Dimension>();
        List<Dimension> dims_V = new ArrayList<Dimension>();
        List<Dimension> dims_Lat = new ArrayList<Dimension>();
        List<Dimension> dims_Lon = new ArrayList<Dimension>();
        List<Dimension> dims_Time = new ArrayList<Dimension>();
        dims_Lat.add(xDim);
        dims_Lon.add(yDim);
        dims_Time.add(zDim);
        dims_U.add(zDim);
        dims_U.add(xDim);
        dims_U.add(yDim);
        dims_V.add(zDim);
        dims_V.add(xDim);
        dims_V.add(yDim);
        Variable dataU = dataFile.addVariable(null, "wind_u", DataType.SHORT,dims_U);
        Variable dataV = dataFile.addVariable(null, "wind_v", DataType.SHORT,dims_V);
        Variable latV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
        Variable lonV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
        Variable timeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
        lonV.addAttribute(new Attribute("units", "degrees_east"));
        latV.addAttribute(new Attribute("units", "degrees_north"));
        dataFile.create();
        Array windS  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},wsd);
		Array windD  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},wdr);
		double[] timeData = new double[realTime];
        int index =0;
        Iterator<Integer> it = set.iterator();
        while(it.hasNext()){
        	int time = it.next();
        	if(timeNum == 0 & time < 8){//如果是第一次写逐小时的文件，则需要补齐前24小时逐小时的时次
        		timeData[index] = (time + 1) * 3 - 2;
        		timeData[index + 1] = (time + 1) * 3 -1;
        		timeData[index + 2] = (time + 1) * 3;
        		index +=3;
        	}else{
        		timeData[index] = (time + 1) * 3;
            	index++;
        	}
        }
        Arrays.sort(timeData);
		Array tiemArray  = Array.factory(DataType.DOUBLE, new int[]{realTime},timeData);
		dataFile.write(dataU, windS);
		System.out.println("u success");
		dataFile.write(dataV, windD);
		System.out.println("v success");
		dataFile.write(timeV, tiemArray);
		dataFile.write(latV, Array.factory(latng));
		dataFile.write(lonV, Array.factory(lonng));
		System.out.println("write netcdf success");
		dataFile.close();
	}
	
	public void WriteNcFile_12h(String outPutPath,String elementName,String element,String fileDate)throws Exception{//写逐12小时非风力风向的nc文件
		
	}
	
	public void WriteWindNcFile_12h(String outPutPath,String elementName,String element,String fileDate)throws Exception{ //写逐12小时风力风向的nc文件
		
	}
	
	public void CopyFile(String outPutPath,String elementName,String element,Date date,String timeStep){  //复制文件
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String LastFileDate = null;   //上一次nc文件的日期
		String NextFileDate = null;   //这一次nc文件的日期
		if(hour>=12){      //判断，如果当前时间为18点以前，则读取08时次的grib2文件，否则读取20时次的grib2文件
			NextFileDate = fileDay+"2000"+timeStep;
			LastFileDate= fileDay+"0800"+timeStep;
		}else{
			NextFileDate = fileDay+"0800"+timeStep;
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.DAY_OF_MONTH, -1);
			date = cal.getTime();
			String LastFileDay = sdf.format(date);
			LastFileDate = LastFileDay + "2000" + timeStep;
		}
		
		FileChannel in = null;  
	    FileChannel out = null;  
	    FileInputStream inStream = null;  
	    FileOutputStream outStream = null;
		for(int t=0;t<2;t++){
			String LastFileName = outPutPath + elementName +"/" + element+ "_" + LastFileDate + "_" + t + ".nc";
			String NextFileName = outPutPath + elementName +"/" + element+ "_" + NextFileDate + "_" + t + ".nc";
			 try {
			    	inStream = new FileInputStream(LastFileName);  
			        outStream = new FileOutputStream(NextFileName);
			        in = inStream.getChannel();
			        out = outStream.getChannel();  
			        in.transferTo(0, in.size(), out);  
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					try {
						in.close();
						out.close();
						inStream.close();
						outStream.close();
					} catch (Exception e2) {
						e2.printStackTrace();
					}
				}
		}
	}
	
	public float ValidData(float data){   //降水量数据极值校验
		return data>999 ? 0.0F : data;
	}
	
	public float ValidTempData(float data){   //温度数据极值校验
		if(data==9999){//当温度为0或者温度<-50,>50的时候也读取上一次的文件
			data = 0;        //暂时先定为0，以后根据流程会读取上一个时次的文件，但是好像纬度格点边缘的数据都是9999.0
		}else{
			data = data - 273.15f;
		}
		return data;
	}
	
	public float ValidRhData(float data){   //相对湿度数据极值校验
		return data>100 || data<0 ? 0.0F : data;
	}
	
	public short DoubleLineInter(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String type) throws Exception{   //双线性插值算法
		float fx = lat_5Km_max - lat_5Km_min;
		float Q11;
		float Q21;
		float Q12;
		float Q22;
		float CorTemp = 0;
		
		if(type.indexOf("temp")>=0){    //如果预报类型为温度，则使用温度的极值检验
			Q11 = ValidTempData(source3[time][0][lat_min][lon_min]);
			Q21 = ValidTempData(source3[time][0][lat_min][lon_max]);
			Q12 = ValidTempData(source3[time][0][lat_max][lon_min]);
			Q22 = ValidTempData(source3[time][0][lat_max][lon_max]);
			int lat = (int) Math.round(lat_1Km * 100);
			int lon = (int) Math.round(lon_1Km * 100)-7000;
//			System.out.println(lat+"--"+lon);
			CorTemp = tempCorrection[0][lat][lon];
		}else{
			Q11 = ValidData(source[time][lat_min][lon_min]);
			Q21 = ValidData(source[time][lat_min][lon_max]);
			Q12 = ValidData(source[time][lat_max][lon_min]);
			Q22 = ValidData(source[time][lat_max][lon_max]);
		}
		float data1 = (lat_5Km_max - lat_1Km) / fx;
		float data2 = (lat_1Km - lat_5Km_min) / fx;
		float R1 = data1 * Q11 + data2 * Q21;
		float R2 = data1 * Q12 + data2 * Q22;
		float P = ((lon_5Km_max - lon_1Km) / (lon_5Km_max - lon_5Km_min))  * R1 + ((lon_1Km - lon_5Km_min) / (lon_5Km_max - lon_5Km_min)) * R2;
//		P = P + CorTemp;
		short Data = new BigDecimal(P * 10 - CorTemp).setScale(0, BigDecimal.ROUND_HALF_UP).shortValue();
//		float Data =  (float) (Math.round(P*10)/10.0);
		return Data;
	}
	
	//双线性插值，风力风向需要从source2里面取值，只为解析风力风向文件
	public float DoubleLineInter2(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String wind){   //双线性插值算法
		float fx = lat_5Km_max - lat_5Km_min;
		float Q11;
		float Q21;
		float Q12;
		float Q22;
		if("u".equals(wind)){
			Q11 = ValidData(source2[time][0][lat_min][lon_min]);
			Q21 = ValidData(source2[time][0][lat_min][lon_max]);
			Q12 = ValidData(source2[time][0][lat_max][lon_min]);
			Q22 = ValidData(source2[time][0][lat_max][lon_max]);
		}else{
			Q11 = ValidData(source3[time][0][lat_min][lon_min]);
			Q21 = ValidData(source3[time][0][lat_min][lon_max]);
			Q12 = ValidData(source3[time][0][lat_max][lon_min]);
			Q22 = ValidData(source3[time][0][lat_max][lon_max]);
		}
		float data1 = (lat_5Km_max - lat_1Km) / fx;
		float data2 = (lat_1Km - lat_5Km_min) / fx;
		float R1 = data1 * Q11 + data2 * Q21;
		float R2 = data1 * Q12 + data2 * Q22;
		float P = ((lon_5Km_max - lon_1Km) / (lon_5Km_max - lon_5Km_min))  * R1 + ((lon_1Km - lon_5Km_min) / (lon_5Km_max - lon_5Km_min)) * R2;
//		short Data = new BigDecimal(P*10).setScale(0, BigDecimal.ROUND_HALF_UP).shortValue();
		float Data =  (float) (Math.round(P*10)/10.0);
		return Data;
	}
	
	//相对湿度双线性插值，返回类型为byte
	public byte DoubleLineInter3(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String type) throws Exception{   //双线性插值算法
		float fx = lat_5Km_max - lat_5Km_min;
		float Q11;
		float Q21;
		float Q12;
		float Q22;
		
		if("cloud".equals(FcType)){   //云量要素用source
			Q11 = ValidRhData(source[time][lat_min][lon_min]);
			Q21 = ValidRhData(source[time][lat_min][lon_max]);
			Q12 = ValidRhData(source[time][lat_max][lon_min]);
			Q22 = ValidRhData(source[time][lat_max][lon_max]);
		}else{
			Q11 = ValidRhData(source3[time][0][lat_min][lon_min]);
			Q21 = ValidRhData(source3[time][0][lat_min][lon_max]);
			Q12 = ValidRhData(source3[time][0][lat_max][lon_min]);
			Q22 = ValidRhData(source3[time][0][lat_max][lon_max]);
		}
		
		float data1 = (lat_5Km_max - lat_1Km) / fx;
		float data2 = (lat_1Km - lat_5Km_min) / fx;
		float R1 = data1 * Q11 + data2 * Q21;
		float R2 = data1 * Q12 + data2 * Q22;
		float P = ((lon_5Km_max - lon_1Km) / (lon_5Km_max - lon_5Km_min))  * R1 + ((lon_1Km - lon_5Km_min) / (lon_5Km_max - lon_5Km_min)) * R2;
		byte Data = new BigDecimal(P).setScale(0, BigDecimal.ROUND_HALF_UP).byteValue();
		return Data;
	}

}
