package weather.nmc.fc.check;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeMap;

import com.sun.org.apache.xalan.internal.xsltc.util.IntegerArray;

import sun.java2d.pipe.SpanShapeRenderer.Simple;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class CheckNmcFc {

	/**
	 * @author Robin
	 * netcdf 数据导出站点数据检验
	 */
	static HashMap<String,BigDecimal[]> stationMap = new HashMap<String,BigDecimal[]>();
	
	static HashMap<String, TreeMap<String, Float>> popMap = new HashMap<String,TreeMap<String, Float>>();
	static HashMap<String, TreeMap<String, Integer>>rhMap = new HashMap<String,TreeMap<String, Integer>>();
	static HashMap<String, TreeMap<String, Float>> tempMap = new HashMap<String,TreeMap<String, Float>>();
	static HashMap<String, TreeMap<String, Float>> windUMap = new HashMap<String,TreeMap<String, Float>>();
	static HashMap<String, TreeMap<String, Float>> windVMap = new HashMap<String,TreeMap<String, Float>>();
	static HashMap<String, TreeMap<String, Integer>> cloudMap = new HashMap<String, TreeMap<String,Integer>>();
	static HashMap<String, TreeMap<String, Integer>> weatherCodeMap = new HashMap<String, TreeMap<String,Integer>>();
	static String que = "9999";
	static Date date;
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		if(args.length>0){  //如果有参数，则读取参数里面的时间
			try {
				date = sdf.parse(args[0]);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}else{
			date = new Date();
		}
		ReadNcFile(date);

	}
	
	static{
		BufferedReader br = null;
		String lineTxt = null;
		String path = "/ser/program/fc_program/stainfo_nat.txt";
//		String path = "stainfo_nat.txt";
		try {
			br=new BufferedReader(new FileReader(path));
			
			while((lineTxt = br.readLine()) != null){
				String []array = lineTxt.split("\\s++");
				String stationId = array[0].trim();
				String lat = array[1].trim();
				String lon = array[2].trim();
				BigDecimal a = new BigDecimal(lon).setScale(2, BigDecimal.ROUND_HALF_UP);
				BigDecimal b = new BigDecimal(lat).setScale(2, BigDecimal.ROUND_HALF_UP);
//				System.out.println(stationId + ":" + a + "-" + b);
//				System.out.println("down!");
				BigDecimal [] bg = new BigDecimal[]{a,b};
				stationMap.put(stationId, bg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(br!=null){
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void ReadNcFile(Date date){
		
//		Date date = new Date();
		
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("开始时间："+time1);
		long begin = System.currentTimeMillis(); 
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat beginSdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat fileSdf = new SimpleDateFormat("yyyy-MM-dd HH");
		String nowDate = sdf.format(date);
		
		InputStream inputStream = CheckNmcFc.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String element = properties.getProperty("element");
		String[] elementArray = element.split(",");
		String outPut = properties.getProperty("outoutPath");
		int hours = date.getHours();
		String fileHour = null;
		if(hours>8&hours<20){
			fileHour = "08";
		}else{
			fileHour = "20";
		}
		Calendar cal = Calendar.getInstance();
		
		String beginDateS = nowDate + fileHour;
		String outPutPath = outPut + "fc" + "_" + nowDate + fileHour + ".dat";
//		String outPutPath = "fc" + "_" + nowDate + fileHour + ".dat";
		try {
			Date beginDate = beginSdf.parse(beginDateS);
			cal.setTime(beginDate);
			
			for(int i=0;i<elementArray.length;i++){
				String elementName = elementArray[i].trim();
//				System.out.println(elementName);
				String pro = properties.getProperty(elementName); 
				String name = pro.split(",")[0];
				String path = pro.split(",")[1];
				
				NetcdfFile ncFile = null;
//				BufferedWriter fw =null;
				
//				fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
				for(int j=0;j<2;j++){
//					String fileName = name + nowDate + fileHour + "00_24003_" + j + ".nc";
					String fileName = path + name + nowDate + fileHour + "00_24003_" + j + ".nc";
//					String fileName = name + nowDate + fileHour + "00_24003_" + j + ".nc";
					System.out.println("file name-->"+ fileName);
					File file = new File(fileName);
					if(!file.exists()){
						continue;
					}
					
					ncFile = NetcdfFile.open(fileName);
					Variable v = ncFile.findVariable(elementName);
					Variable lat = ncFile.findVariable("lat");
					Variable lon = ncFile.findVariable("lon");
					Variable time = ncFile.findVariable("time");
					Array lonArray = lon.read();
					Array latArray = lat.read();
					Array timeArray = time.read();
					String timeLength = String.valueOf(timeArray.getSize()-1);
					String lonLength = String.valueOf(lonArray.getSize()-1);
					String latLength = String.valueOf(latArray.getSize()-1);
					String section = "0:" + timeLength + ",0:" + latLength + ",0:" + lonLength;
//					System.out.println(section);
					Array data = v.read(section);
//					int count =0;
						
					if("pop".equals(elementName)){
						short[][][] source = (short[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Float> popDataMap = new TreeMap<String, Float>();
							
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
//							count++;
//							System.out.println(count);
							for(int k=0;k<timeArray.getSize();k++){
								float popf =  new BigDecimal(source[k][latNum][lonNum]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
//									float popf = data.getFloat(k) / 10;
//									String pop = String.valueOf(popf);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								popDataMap.put(fcDateS, popf);
							}
							
							if(popMap.containsKey(stationId)){
								TreeMap<String, Float> map = popMap.get(stationId);
								map.putAll(popDataMap);
								popMap.put(stationId, map);
							}else{
								popMap.put(stationId, popDataMap);
							}
						}
						source = null;
					}else if("rh".equals(elementName)){
						byte[][][] source = (byte[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Integer> rhDataMap = new TreeMap<String, Integer>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
//							count++;
//							System.out.println(count);
							for(int k=0;k<timeArray.getSize();k++){
								
								int rh = (int) source[k][latNum][lonNum];
//									int rh = data.getInt(k);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								rhDataMap.put(fcDateS, rh);
							}
							if(rhMap.containsKey(stationId)){
								TreeMap<String, Integer> map = rhMap.get(stationId);
								map.putAll(rhDataMap);
								rhMap.put(stationId, map);
							}else{
								rhMap.put(stationId, rhDataMap);
							}
						}
						source = null;
					}else if("temp".equals(elementName)){
						short[][][] source = (short[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Float> tempDataMap = new TreeMap<String, Float>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
//							count++;
//							System.out.println(count);
							for(int k=0;k<timeArray.getSize();k++){
								float tempf =  new BigDecimal(source[k][latNum][lonNum]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
//									float tempf = data.getFloat(k) / 10;
//									String pop = String.valueOf(popf);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								tempDataMap.put(fcDateS, tempf);
							}
							if(tempMap.containsKey(stationId)){
								TreeMap<String, Float> map = tempMap.get(stationId);
								map.putAll(tempDataMap);
								tempMap.put(stationId, map);
							}else{
								tempMap.put(stationId, tempDataMap);
							}
						}
						source = null;
					}else if("wind_u".equals(elementName)){
						short[][][] source = (short[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Float> windUDataMap = new TreeMap<String, Float>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
//							count++;
//							System.out.println(count);
							for(int k=0;k<timeArray.getSize();k++){
								float tempf = source[k][latNum][lonNum];
//									float tempf = data.getFloat(k) / 10;
//									String pop = String.valueOf(popf);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								windUDataMap.put(fcDateS, tempf);
							}
							if(windUMap.containsKey(stationId)){
								TreeMap<String, Float> map = windUMap.get(stationId);
								map.putAll(windUDataMap);
								windUMap.put(stationId, map);
							}else{
								windUMap.put(stationId, windUDataMap);
							}
						}
						source = null;
					}else if("wind_v".equals(elementName)){
						short[][][] source = (short[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Float> windVDataMap = new TreeMap<String, Float>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
//							count++;
//							System.out.println(count);
							for(int k=0;k<timeArray.getSize();k++){
								float tempf = source[k][latNum][lonNum];
//									float tempf = data.getFloat(k) / 10;
//									String pop = String.valueOf(popf);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								windVDataMap.put(fcDateS, tempf);
							}
							if(windVMap.containsKey(stationId)){
								TreeMap<String, Float> map = windVMap.get(stationId);
								map.putAll(windVDataMap);
								windVMap.put(stationId, map);
							}else{
								windVMap.put(stationId, windVDataMap);
							}
						}
						source = null;
					}else if("cloud".equals(elementName)){
						byte[][][] source = (byte[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Integer> cloudDataMap = new TreeMap<String, Integer>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							for(int k=0;k<timeArray.getSize();k++){
								int cloudf =  source[k][latNum][lonNum];
//									float tempf = data.getFloat(k) / 10;
//									String pop = String.valueOf(popf);
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								cloudDataMap.put(fcDateS, cloudf);
							}
							if(cloudMap.containsKey(stationId)){
								TreeMap<String, Integer> map = cloudMap.get(stationId);
								map.putAll(cloudDataMap);
								cloudMap.put(stationId, map);
							}else{
								cloudMap.put(stationId, cloudDataMap);
							}
						}
					}else if("weatherCode".equals(elementName)){
						byte[][][] source = (byte[][][]) data.copyToNDJavaArray();
						data = null;
						System.gc();
						for(String stationId : stationMap.keySet()){
							TreeMap<String, Integer> weatherCodeDataMap = new TreeMap<String, Integer>();
							BigDecimal [] bg = stationMap.get(stationId);
							BigDecimal a = bg[0].subtract(new BigDecimal(lonArray.getFloat(0)));   //经度
							BigDecimal b = bg[1].subtract(new BigDecimal(latArray.getFloat(0)));   //纬度
							BigDecimal c = new BigDecimal(0.01).setScale(2, BigDecimal.ROUND_HALF_UP);
							int lonNum = a.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							int latNum = b.divide(c, 0, BigDecimal.ROUND_HALF_UP).intValue();
							for(int k=0;k<timeArray.getSize();k++){
								int weatherCodef = source[k][latNum][lonNum];
								int timeNum = timeArray.getInt(k);
								cal.add(Calendar.HOUR_OF_DAY, timeNum);
								Date fcDate = cal.getTime();
								cal.setTime(beginDate);
								String fcDateS = fileSdf.format(fcDate);
								weatherCodeDataMap.put(fcDateS, weatherCodef);
							}
							if(weatherCodeMap.containsKey(stationId)){
								TreeMap<String, Integer> map = weatherCodeMap.get(stationId);
								map.putAll(weatherCodeDataMap);
								weatherCodeMap.put(stationId, map);
							}else{
								weatherCodeMap.put(stationId, weatherCodeDataMap);
							}
						}
					}
					ncFile.close();
				}
			}
			System.gc();  //建议jvm清理内存空间
		} catch (Exception e) {
			e.printStackTrace();
		}
		writeStationFile(outPutPath,beginDateS);    //写文件
		
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		System.out.println("结束时间："+time2);
		System.out.println("运行时间："+(end-begin)+"ms");
	}
	
	public static void writeStationFile(String path,String date){
		
//		path = "check.dat";
		
		BufferedWriter fw = null;
		SimpleDateFormat beginSdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		int timeNum = 96;
		int step = 3;
		Calendar cal = Calendar.getInstance();
		
		TreeMap<String, Float> popDataMap = null;
		TreeMap<String, Integer> rhDataMap = null;
		TreeMap<String, Float> tempDataMap = null;
		TreeMap<String, Float> windUDataMap = null;
		TreeMap<String, Float> windVDataMap = null;
		TreeMap<String, Integer> cloudDataMap = null;
		TreeMap<String, Integer> weatherCodeDataMap = null;
		try {
			Date beginDate = beginSdf.parse(date);
			cal.setTime(beginDate);
			fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
			
			fw.append(formatstr2("站数"));
			fw.append(formatstr2("要素个数"));
			fw.append(formatstr2("缺测指示码"));
			fw.newLine();
			fw.append(formatstr2(String.valueOf(stationMap.size())));
			fw.append(formatstr2("7"));
			fw.append(formatstr2(que));
			fw.newLine();
//			fw.append("LST,TEMP,RAIN,RH,WIND_SPEED,WIND_DIR,CLOUD,WEATHER_CODE");
			
			fw.append(formatstr("LST"));
			fw.append(formatstr2("TEM"));
			fw.append(formatstr2("PRE"));
			fw.append(formatstr2("RHU"));
			fw.append(formatstr2("WIN_S"));
			fw.append(formatstr2("WIN_D"));
			fw.append(formatstr2("CLO_Cov"));
			fw.append(formatstr2("WEATHER"));
			
			fw.newLine();
			for(String stationId : stationMap.keySet()){
				fw.append(stationId);
				fw.newLine();
				int TimeStep = 1;
				for(int i=1;i<=timeNum;i++){
					if(i<=24){
						TimeStep = i;
					}else{
						TimeStep += 3;
					}
					
					cal.add(Calendar.HOUR_OF_DAY, TimeStep);
					Date fcDate = cal.getTime();
					cal.setTime(beginDate);
					String fcDateS = sdf.format(fcDate);
					
					String temp = null;
					String rain = null;
					String rh = null;
					String wind_u = null;
					String wind_v = null;
					String cloud = null;
					String weatherCode = null;
					
					if(tempMap.containsKey(stationId)){
						tempDataMap = tempMap.get(stationId);
						if(tempDataMap.containsKey(fcDateS)){
							temp = String.valueOf(tempDataMap.get(fcDateS));
						}else{
							temp = "9999";
						}
					}else{
						temp = "9999";
					}
					
					if(popMap.containsKey(stationId)){
						popDataMap = popMap.get(stationId);
						if(popDataMap.containsKey(fcDateS)){
							rain = String.valueOf(popDataMap.get(fcDateS));
						}else{
							rain = "9999";
						}
					}else{
						rain = "9999";
					}
					
					if(rhMap.containsKey(stationId)){
						rhDataMap = rhMap.get(stationId);
						if(rhDataMap.containsKey(fcDateS)){
							rh = String.valueOf(rhDataMap.get(fcDateS));
						}else{
							rh = "9999";
						}
					}else{
						rh = "9999";
					}
					
					if(windUMap.containsKey(stationId)){
						windUDataMap = windUMap.get(stationId);
						if(windUDataMap.containsKey(fcDateS)){
							wind_u = String.valueOf(windUDataMap.get(fcDateS));
						}else{
							wind_u = "9999";
						}
					}else{
						wind_u = "9999";
					}
					
					if(windVMap.containsKey(stationId)){
						windVDataMap = windVMap.get(stationId);
						if(windVDataMap.containsKey(fcDateS)){
							wind_v = String.valueOf(windVDataMap.get(fcDateS));
						}else{
							wind_v = "9999";
						}
					}else{
						wind_v = "9999";
					}
					
					//默认认为风力风向为9999
					String windSpeed = "9999";   //风速
					String windDir = "9999"; 	 //风向
					if(!"9999".equals(wind_u) & !"9999".equals(wind_v)){   //当风力风向都为有效值时
						float u = Float.valueOf(wind_u);
						float v = Float.valueOf(wind_v);
						double speed = 0;
						double dir = 0;
						
						if(v==0){
							if(u>0){
								speed = u;
								dir = 90;
							}else if(u<0){
								speed = -u;
								dir = 270;
							}else{
								speed = 0;
								dir = 0;
							}
						}else if(u>=0&v>0){
							speed = Math.sqrt(u * u + v * v);   //风速
							dir =Math.toDegrees(Math.atan(u/v));  //风向
						}else if(u>=0&v<0){
							speed = Math.sqrt(u * u + v * v);   //风速
							dir =180 + Math.toDegrees(Math.atan(u/v));  //风向
						}else if(u<0&v<0){
							speed = Math.sqrt(u * u + v * v);   //风速
							dir =180 + Math.toDegrees(Math.atan(u/v));  //风向
						}else if(u<0&v>0){
							speed = Math.sqrt(u * u + v * v);   //风速
							dir =360 + Math.toDegrees(Math.atan(u/v));  //风向
						}
						windSpeed = new BigDecimal(speed).setScale(1, BigDecimal.ROUND_HALF_UP).toString();
						windDir = new BigDecimal(dir).setScale(0, BigDecimal.ROUND_HALF_UP).toString();
					}
					
					if(cloudMap.containsKey(stationId)){
						cloudDataMap = cloudMap.get(stationId);
						if(cloudDataMap.containsKey(fcDateS)){
							cloud = String.valueOf(cloudDataMap.get(fcDateS));
						}else{
							cloud = "9999";
						}
					}else{
						cloud = "9999";
					}
					
					if(weatherCodeMap.containsKey(stationId)){
						weatherCodeDataMap = weatherCodeMap.get(stationId);
						if(weatherCodeDataMap.containsKey(fcDateS)){
							weatherCode = String.valueOf(weatherCodeDataMap.get(fcDateS));
						}else{
							weatherCode = "9999";
						}
					}else{
						weatherCode = "9999";
					}
//					fw.append(formatstr2(String.valueOf(TimeStep)));
					fw.append(formatstr(fcDateS));
					fw.append(formatstr2(temp));
					fw.append(formatstr2(rain));
					fw.append(formatstr2(rh));
					fw.append(formatstr2(windSpeed));
					fw.append(formatstr2(windDir));
					fw.append(formatstr2(cloud));
					fw.append(formatstr2(weatherCode));
					fw.newLine();
				}
			}
			fw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(fw!=null){
				try {
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private static String formatstr(String str){
		byte[] strByte = str.trim().getBytes();
		StringBuilder sb = new StringBuilder();
		byte[] testBytes=str.length()>1?str.substring(0,1).getBytes():str.getBytes();
		int len=0;
		if(testBytes.length>=3){
			len= 20-strByte.length*2/3;
		}else if(testBytes.length>=2){
			len= 20-strByte.length;
		}else if(testBytes.length>=1){
			len= 20-strByte.length;
		}else{
			len=20;
		}
		sb.append(str.trim());
		for (int i = 0; i < len; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}
	
	
	private static String formatstr2(String str){
		byte[] strByte = str.trim().getBytes();
		StringBuilder sb = new StringBuilder();
		byte[] testBytes=str.length()>1?str.substring(0,1).getBytes():str.getBytes();
		int len=0;
		if(testBytes.length>=3){
			len= 10-strByte.length*2/3;
		}else if(testBytes.length>=2){
			len= 10-strByte.length;
		}else if(testBytes.length>=1){
			len= 10-strByte.length;
		}else{
			len=10;
		}
		sb.append(str.trim());
		for (int i = 0; i < len; i++) {
			sb.append(" ");
		}
		return sb.toString();
	}
	
}
