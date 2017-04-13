package weather.nmc.fc.onehour;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;

public class WeatherCode3h21h implements Runnable {
	
	private static String elementName = "WEATHER";
	private static String dataType = "nmc"; //数据类型
	private static String timeDes = "1h"; //时间描述
	private static int Offset = 0; //偏移量，默认为0
	private float startLat = 0.0F;
	private float startLon = 70.0F;
	private float endLat = 60.0F;
	private float endLon = 140.0F;	
	private float latLonStep = 0.01F;
	private static int dayNum; //天数
	private static int nowDay;
	static int ThreadNum = 10;
	private static ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<Integer>();
	private static Map<String, HashMap<Integer, List<Double>>> ocfMap = new HashMap<String, HashMap<Integer,List<Double>>>();
	private static int timeNum;
	private static short[][][] popSource;
	private static short[][][] tempSource;
	private static byte[][][] cloudSource;
	private static int[][][] stationIdSource;
	private static double[][][] distanceSource;
	private static NetcdfFile popNcFile = null;
	private static NetcdfFile tempNcFile = null;
	private static NetcdfFile cloudNcFile = null;
	private static NetcdfFile stationFile = null;
	
	private static int timeRange;
	private static int latRange;
	private static int lonRange;
	
	private static double[] time;
	private static Array refTime;
	private static float[] lon;
	private static float[] lat;
	private static byte[] data;
	static Logger logger = LogManager.getLogger(WeatherCode3h21h.class.getName());
	@Override
	public void run() {
		WeatherCode3h21h wc = new WeatherCode3h21h();
		wc.DataCal();
	}
	/**
	 * 多线程写天气现象nc文件
	 * @param args
	 */
	public static void main(String[] args) {
		WeatherCode3h21h wc = new WeatherCode3h21h();
		Date date = null;
		SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
		if(args.length<1){
			dayNum = 1;
		}else{
			dayNum = Integer.valueOf(args[0]);
		}
		if(args.length>1){
			try {
				date = sdfh.parse(args[1]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}else{
			date = new Date();
		}
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("开始时间："+time1);
		logger.error("weatherCode-->开始时间："+time1);
		long begin = System.currentTimeMillis();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		int hour = date.getHours();
		String nowDateS = sdf.format(date);
		InputStream inputStream = WeatherCode3h21h.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String pro = properties.getProperty("weatherCode_1h");
		String popPath = pro.split(",")[0]; //降水量nc文件path
		String tempPath = pro.split(",")[1]; //温度nc文件path
		String cloudPath = pro.split(",")[2]; //云量nc文件path
		String outPath = properties.getProperty("OutPutPath_1h");
		String ocfPath = properties.getProperty("ocfPath");
		String ocfHead = properties.getProperty("ocfFileHead");
		String ocfEnd = properties.getProperty("ocfFileEnd");
		String disFile = properties.getProperty("disFile");
		
		String fileHour = null;
		if(hour>3&hour<12){  //如果当前小时为3-12点，则认为降水量和温度的预报文件为08，否则认为预报文件为20
			fileHour = "0800";
		}else{
			fileHour = "2000";
		}
		String fileDate = nowDateS + fileHour;
		
		boolean ocf = wc.readOcfFile(ocfPath, ocfHead, nowDateS,ocfEnd,hour);
		if(ocf){
			System.out.println("ocf file read success");
		}else{
			System.out.println("failed to read ocf file");
		}
		try {
			for(timeNum = 0;timeNum<dayNum;timeNum++){
				nowDay = timeNum;
				boolean flag = wc.ReadNcFile(popPath, tempPath, cloudPath, disFile, fileDate);
				if(!flag){//如果文件不同时存在
					continue;
				}
				System.gc();
				for(int j = 0;j < timeRange;j++){ //将时次放入队列中   ,
					queue.add(j);
				}
				ExecutorService exe = Executors.newFixedThreadPool(ThreadNum);
				for (int i = 0; i < ThreadNum; i++){
					exe.execute(wc);
		        }
				exe.shutdown();
				while (true) {
					if (exe.isTerminated()) {
						wc.WriteNcFile(outPath,fileDate);
						break;
					}
				}
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
		
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		logger.error("weatherCode-->结束时间："+time2);
		logger.error("weatherCode-->运行时间："+(end-begin)+"ms");
		System.out.println("结束时间："+time2);
		System.out.println("运行时间："+(end-begin)+"ms");
	}
	
	public void DataCal(){
		while(!queue.isEmpty()){
			int time = queue.poll();
			System.out.println(time);
			for(int i=0;i<latRange;i++){
				for(int k=0;k<lonRange;k++){
//					float pop = new BigDecimal(popSource[t][i][k]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
					float p = Float.valueOf(String.valueOf(popSource[time][i][k]));
					float pop=(float) (Math.round(p*10)/100.0);
//					float pop = Math.round((Float.valueOf(String.valueOf(popSource[t][i][k]))) * 10) / 10;
					byte code = 0;
					if(pop>0){  //  有降水,看温度
//						System.out.println(pop);
						short temp = (short) (tempSource[time][i][k] / 10);
						if(temp <= -2){  //雪
							if(pop <= 0.2){  //小雪
								code = 14;
							}else if(pop > 0.2 & pop <= 0.5){ //中雪
								code = 15;
							}else if(pop > 0.5 & pop <= 1.3){ //大雪
								code = 16;
							}else{ //暴雪
								code = 17;
							}
						}else if(temp <= 2 & temp > -2){  //雨夹雪
							code = 6;
						}else{//雨
							if(pop<=1){  //小雨
								code = 7;
							}else if(pop > 1 & pop <= 3.3){ //中雨
								code = 8;
							}else if(pop > 3.3 & pop <= 6.6){ //大雨
								code = 9;
							}else if(pop > 6.6 & pop <= 16.6){ //暴雨
								code = 10;
							}else if(pop > 16.6 & pop <= 33.3){ //大暴雨
								code = 11;
							}else{  //特大暴雨
								code = 12;
							}
						}                                                                                                
					}else{  //无降水，看云量
//						float cloud = new BigDecimal(cloudSource[t][i][k]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
//						float cloud = (Float.valueOf(String.valueOf(cloudSource[t][i][k]))) / 10;
						float c = Float.valueOf(String.valueOf(cloudSource[time][i][k]));
						float cloud=(float) (Math.round(c*10)/10.0);
						if(cloud <= 20){ //晴天
							code = 0;
						}else if(cloud > 20 & cloud <= 80){ //多云
							code = 1;
						}else{ //阴天
							code = 2;
						}
					}
//					System.out.println(code);
					//站点融合
					
					
					int stationId = stationIdSource[0][i][k];
					String idStr = String.valueOf(stationId);
					double distance = 999;
					if(stationId > 0){
						distance = distanceSource[0][i][k];
						if(distance <= 3){
							if(ocfMap.containsKey(idStr)){
								byte ocfCode = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue();
//								if((ocfCode>=0 & ocfCode<=31) || ocfCode == 53 || ocfCode == 301 || ocfCode == 302){    //modified by robin 2017-1-18   注释ocf霾预报
									if((ocfCode>=0 & ocfCode<=31) || ocfCode == 301 || ocfCode == 302){
									code = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue(); //ocf预报从第一个时次为07-08，所以要+1
									short rain = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(0)* 10).shortValue();
									popSource[time][i][k] = rain;
								}
							}
						}else{
							if(ocfMap.containsKey(idStr)){
								byte ocfCode = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue();
								//modified by robin 2017-1-18   注释ocf霾预报
								/*if(ocfCode == 53){  //ocf预报为霾
									code = ocfCode;
									popSource[time][i][k] = 0;
								}
								else if(ocfCode == 18){ //ocf预报为雾*/
								if(ocfCode == 18){
									if(distance < 20){
										code = ocfCode;
										popSource[time][i][k] = 0;
									}
								}else if(ocfCode == 29){  //ocf预报为沙尘
									if(distance <= 10){
										code = ocfCode;
										popSource[time][i][k] = 0;
									}
								}else if(ocfCode == 19){  //ocf预报为冻雨    add by robin 2017-1-20  添加冻雨订正
									if(distance <= 10){
										code = ocfCode;
										popSource[time][i][k] = 0;
									}
								}
							}
						}
					}
					
/*					if(stationId == 54511 && code >20){
						System.out.println(time + "--" + i + "--" + k + "--" + distance + "--" + code);
					}
*/					if((code>=0 & code<=31) || code == 53 || code == 301 || code == 302){
						data[time * latRange * lonRange + i * lonRange + k] = code;
					}else{
						data[time * latRange * lonRange + i * lonRange + k] = 0;
					}
					
				}
			}
		}
	}
	
	/**
	 * 
	 * @param popPath
	 * @param tempPath
	 * @param cloudPath
	 * @param nowDateS
	 * @param fileHour
	 * @throws Exception
	 */
	public boolean ReadNcFile(String popPath,String tempPath,String cloudPath,String disPath, String fileDate) throws Exception{
		
//		popPath = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\nmc_1h_PRE_";
//		tempPath = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\nmc_1h_TEM_";
//		cloudPath = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\nmc_1h_CLOUD_";
		String popFileName = getNcFileName(popPath, fileDate,1);
		String tempFileName = getNcFileName(tempPath, fileDate,1);
		String cloudFileName = getNcFileName(cloudPath, fileDate,0);
		
		System.out.println(popFileName);
		System.out.println(tempFileName);
		System.out.println(cloudFileName);
		
//		String stationFileName = "distance.nc";
		String stationFileName = disPath + "distance.nc";
		
		File popFile = new File(popFileName);
		File tempFile = new File(tempFileName);
		File cloudFile = new File(cloudFileName);
		if(!popFile.exists() || !tempFile.exists() || !cloudFile.exists()){   //若有一个文件不存在
			System.out.println("source file not exist!");
			return false;  
		}
		popNcFile = NetcdfFile.open(popFileName);
		tempNcFile = NetcdfFile.open(tempFileName);
		cloudNcFile = NetcdfFile.open(cloudFileName);
		stationFile = NetcdfFile.open(stationFileName);
		
		Variable popV = popNcFile.findVariable("PRE");
		Variable tempV = tempNcFile.findVariable("TEM");
		Variable cloudV = cloudNcFile.findVariable("CLOUD");
		Variable distanceV = stationFile.findVariable("distance");
		Variable stationIdV = stationFile.findVariable("name");
		
		Variable latV = popNcFile.findVariable("lat");
		Variable lonV = popNcFile.findVariable("lon");
		Variable timeV = popNcFile.findVariable("time");
		Variable refTimeV = popNcFile.findVariable("reftime");
		Array lonArray = lonV.read();
		Array latArray = latV.read();
		Array timeArray = timeV.read();
		Array refTimeArray = refTimeV.read();
		String timeLength = String.valueOf(timeArray.getSize()-1);
		String lonLength = String.valueOf(lonArray.getSize()-1);
		String latLength = String.valueOf(latArray.getSize()-1);
		String section = "0:" + timeLength + ",0:" + latLength + ",0:" + lonLength;
		String stationSection = "0," + "0:" + latLength + ",0:" + lonLength; 
		System.out.println(section);
		
		Array tempData = tempV.read(section);
		Array popData = popV.read(section);
		Array cloudData = cloudV.read(section);
		Array stationIdData = stationIdV.read(stationSection);
		Array distanceData = distanceV.read(stationSection);
		
		time = (double[]) timeArray.copyTo1DJavaArray();
		refTime = refTimeArray.copy();
		lon = (float[]) lonArray.copyTo1DJavaArray();
		lat = (float[]) latArray.copyTo1DJavaArray();
		
		timeRange = time.length;
		latRange = lat.length;
		lonRange = lon.length;
		data = new byte[timeRange * latRange * lonRange];
		popSource = (short[][][]) popData.copyToNDJavaArray();
		tempSource = (short[][][]) tempData.copyToNDJavaArray();
		cloudSource = (byte[][][]) cloudData.copyToNDJavaArray();
		stationIdSource = (int[][][]) stationIdData.copyToNDJavaArray();
		distanceSource = (double[][][]) distanceData.copyToNDJavaArray();
		
		popData = null;
		tempData = null;
		cloudData = null;
		
		stationIdData = null;
		distanceData = null;
		popNcFile.close();
		tempNcFile.close();
		cloudNcFile.close();
		stationFile.close();
		return true;
	}
	
	/**
	 * 
	 * @param outPath
	 * @param nowDateS
	 * @param fileHour
	 * @throws Exception
	 */
	public void WriteNcFile(String outPath,String fileDate) throws Exception{
//		String outPutPath = elementName + "_" + nowDateS + fileHour + "00_24003_" + timeNum + ".nc";
//		String outPutPath = outPath + elementName + "/" + elementName + "_" + nowDateS + fileHour + "00_24003_" + timeNum + ".nc";
		StringBuffer filenameSB = new StringBuffer();
		filenameSB.append(outPath)  //路径
		.append(elementName)           //要素路径
		.append("/")          
		.append(dataType)//数据类型
		.append("_")
		.append(timeDes)//时间描述
		.append("_")
		.append(elementName)//要素名称
		.append("_")
		.append(24 * nowDay + 1)//起始时间
		.append("_")
		.append(0)//z起始
		.append("_")
		.append(new BigDecimal(startLat).setScale(0))//纬度起始
		.append("_")
		.append(new BigDecimal(startLon).setScale(0))//经度起始
		.append("_")
		.append(1)//时间间隔
		.append("_")
		.append(0)//z间隔
		.append("_")
		.append(latLonStep)//纬度间隔
		.append("_")
		.append(latLonStep)//经度间隔
		.append("_")
		.append((nowDay + 1) * 24)//时间结束
		.append("_")
		.append(0)//z结束
		.append("_")
		.append(new BigDecimal(endLat).setScale(0))//纬度结束
		.append("_")
		.append(new BigDecimal(endLon).setScale(0))//经度结束
		.append("_")
		.append(Offset)//存储精度
		.append("_")
		.append(fileDate)//时间
		.append(".nc");
		System.out.println("netcdf out put path-->" + filenameSB.toString());
		
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//这种类型写的文件访问速度最快
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
	    dataFile = NetcdfFileWriter.createNew(version, filenameSB.toString(),chunker);
		
	    Dimension xDim = dataFile.addDimension(null, "lat", latRange);
        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
        Dimension zDim = dataFile.addDimension(null, "time", timeRange);
        Dimension refDim = dataFile.addDimension(null, "reftime", timeRange);
        List<Dimension> dims_Element = new ArrayList<Dimension>();
        List<Dimension> dims_Lat = new ArrayList<Dimension>();
        List<Dimension> dims_Lon = new ArrayList<Dimension>();
        List<Dimension> dims_Time = new ArrayList<Dimension>();
        List<Dimension> dims_RefTime = new ArrayList<Dimension>();
        dims_Lat.add(xDim);
        dims_Lon.add(yDim);
        dims_Time.add(zDim);
        dims_RefTime.add(refDim);
        dims_Element.add(zDim);
        dims_Element.add(xDim);
        dims_Element.add(yDim);	
		
//        double[] timeData = new double[timeRange];
        Variable dataV = dataFile.addVariable(null, elementName, DataType.BYTE,dims_Element);
        Variable latCodeV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
        Variable lonCodeV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
        Variable timeCodeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
        Variable refTimeV = dataFile.addVariable(null, "reftime", DataType.STRING,dims_RefTime);
        lonCodeV.addAttribute(new Attribute("units", "degrees_east"));
        latCodeV.addAttribute(new Attribute("units", "degrees_north"));
        dataFile.create();
        
        System.out.println("开始写文件");
		Array dataArray  = Array.factory(DataType.BYTE, new int[]{timeRange,latRange,lonRange},data);
		dataFile.write(dataV, dataArray);
//		Array timeCodeArray = Array.factory(DataType.DOUBLE, new int[]{timeRange},timeData);
		dataFile.write(timeCodeV, Array.factory(time));
		dataFile.write(refTimeV, refTime);
//		dataFile.write(refTimeV, Array.factory(refTime));
		dataFile.write(latCodeV, Array.factory(lat));
		dataFile.write(lonCodeV, Array.factory(lon));
		System.out.println("write netcdf success!!!");
		dataFile.close();
		
	}
	
	public String getNcFileName(String elementPath,String fileData, int offset ){
		
		StringBuffer sb = new StringBuffer();
		sb.append(elementPath)
//		.append(timeDes)//时间描述
//		.append("_")
//		.append(elementName)//要素名称
//		.append("_")
		.append(24 * nowDay + 1)//起始时间
		.append("_")
		.append(0)//z起始
		.append("_")
		.append(new BigDecimal(startLat).setScale(0))//纬度起始
		.append("_")
		.append(new BigDecimal(startLon).setScale(0))//经度起始
		.append("_")
		.append(1)//时间间隔
		.append("_")
		.append(0)//z间隔
		.append("_")
		.append(latLonStep)//纬度间隔
		.append("_")
		.append(latLonStep)//经度间隔
		.append("_")
		.append((nowDay + 1) * 24)//时间结束
		.append("_")
		.append(0)//z结束
		.append("_")
		.append(new BigDecimal(endLat).setScale(0))//纬度结束
		.append("_")
		.append(new BigDecimal(endLon).setScale(0))//经度结束
		.append("_")
		.append(offset)//存储精度
		.append("_")
		.append(fileData)//时间
		.append(".nc");
		return sb.toString();
	}
	
	/**
	 * 读取ocf原文件
	 * @param path
	 * @param fileHead
	 * @param fcDate
	 * @return
	 */
	public boolean readOcfFile(String path,String fileHead,String fcDate,String fileEnd,int hour){
		String fileName_06 = null;
		String fileName_08 = null;
		String fileName_12 = null;
		String fileName_20 = null;
		File file = null;
		if(hour <= 12 ){
			fileName_06 = path + fileHead + fcDate + "0600" + fileEnd;
			fileName_08 = path + fileHead + fcDate + "0800" + fileEnd;
			fileName_12 = path + fileHead + fcDate + "1200" + fileEnd;
			System.out.println(fileName_06);
			System.out.println(fileName_08);
			System.out.println(fileName_12);
			
			file = new File(fileName_12);
			if(!file.exists()){
				file = new File(fileName_08);
				if(!file.exists()){
					file = new File(fileName_06);
					if(!file.exists()){
						return false;
					}
				}
			}
		}else{
			fileName_20 = path + fileHead + fcDate + "2000" + fileEnd;
			file = new File(fileName_20);
			if(!file.exists()){
				return false;
			}
		}
		System.out.println("ocf file-->"+file);
		BufferedReader br = null;
		String lineTxt = null;
		try {
			br = new BufferedReader(new FileReader(file));
//			br = new BufferedReader(new FileReader("C:\\Users\\Robin\\Desktop\\grib2\\nc\\MSP3_PMSC_OCF1H_ME_L88_GLB_201612010800_00000-36000.TXT"));
			int lineNum = 0;
			String id = null;
			HashMap<Integer, List<Double>> infoMap = new HashMap<Integer, List<Double>>();
			while((lineTxt = br.readLine()) != null){
				if(lineNum<3){
					lineNum++;
					continue;
				}
				String[] array = lineTxt.split("\\s++");
				int length = array.length;
				if(length<=10){//站号行
					if(id!=null){
						ocfMap.put(id, infoMap);
						infoMap = new HashMap<Integer, List<Double>>();
					}
					id = array[0];
					continue;
				}else{
					List<Double> list = new ArrayList<Double>();
					int time = Integer.valueOf(array[1]);
					if(time > 72){  //只读逐小时的数据
						continue;
					}else{
						double rain = Double.valueOf(array[7]);
						double weather = Double.valueOf(array[13]);
						list.add(rain);
						list.add(weather);
						infoMap.put(time, list);
					}
				}
			}
//			System.out.println(ocfMap.get("54511"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
}
