package weather.nmc.temp.error;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;

public class GetPointAvgError {

	/**
	 * @author Robin
	 * ��ȡ���10��ƽ�����,�����д��һ��nc�ļ����������¶�Ԥ��ʱ����
	 */
	
	static float beginLat = 0.0f;
	static float endLat = 60.0f;
	static float beginLon = 70.0f;
	static float endLon = 140.0f;
	static float step = 0.01f;
	static int latGrid = 6001;
	static int lonGrid = 7001;
	static HashMap<String, String> sectionMap;
//	static HashMap<String, int[]> dataMap;
//	static HashMap<String, Integer> obsMap;
	static HashMap<String, Float> errorMap;
	static BufferedWriter fw = null;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		GetError();
//		calculateAvg("139.91,59.99");
		test();
//		getAvg();
//		listTest();
	}
	
	public static void test(){
		
		Date date=new Date();
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("��ʼʱ�䣺"+time1);
		long begin = System.currentTimeMillis(); 
		
		String path = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\out.txt";
//		String path = "/ser/program/fc_program/out.txt";
		BufferedReader br = null;
		String lineTxt = null;
		sectionMap = new HashMap<String, String>();     //��γ�ȱ����Ϊkey��վ����Ϊvalue
		try {
			br = new BufferedReader(new FileReader(path));
			while((lineTxt = br.readLine()) != null){
				String []array = lineTxt.split(",");
//				sectionMap.put(key, value)
				String a = array[0]; //վ��
				String b = array[1]; //γ�ȱ��
				String c = array[2]; //���ȱ��
				String key = b+","+c;
				sectionMap.put(key, a);
//				sectionSet.add(lineTxt);
			}
		} catch (Exception e) {
			System.out.println(lineTxt);
			e.printStackTrace();
		}
		getAvg();
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		System.out.println("����ʱ�䣺"+time2);
		System.out.println("����ʱ�䣺"+(end-begin)+"ms");
	}
	
	/*public static void GetError(){
		Map map = new HashMap();
		sectionMap = new HashMap<String, String>();
		String path ="C:\\Users\\Robin\\Desktop\\grib2\\nc\\tmin_extreme_station.dat";
		String path1 ="C:\\Users\\Robin\\Desktop\\grib2\\nc\\out.txt";
		
		BufferedReader br = null;
		String lineTxt = null;
		try {
			fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path1)));
			br = new BufferedReader(new FileReader(path));
			while((lineTxt = br.readLine()) != null){
				String[] array = lineTxt.split("\\s+");
				String stationId = array[0];
				if("station".equals(stationId)){
					continue;
				}
				String lat = array[1];
				String lon = array[2];
				
//				String data = array[3];
				String lonB = new BigDecimal(lon).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String latB = new BigDecimal(lat).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String key = lonB + "," + latB;
				map.put(key, stationId);
			}
//			System.out.println(map.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		//���þ�γ�ȵķ�Χ������������ѭ��ÿһ����γ�ȱ�����
		int count = 0;
		for(float i = beginLat; i<=endLat; i+=step){
			for(float j = beginLon; j<=endLon; j+=step){
				float lon1 = j-0.01f;
				float lon2 = j+0.01f;
				float lat1 = i-0.01f;
				float lat2 = i+0.01f;
				String a = new BigDecimal(lon1).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String b = new BigDecimal(lon1).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(i).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String c = new BigDecimal(lon1).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat2).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String d = new BigDecimal(j).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String e = new BigDecimal(j).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(i).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String f = new BigDecimal(j).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat2).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String g = new BigDecimal(lon2).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat1).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String h = new BigDecimal(lon2).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(i).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				String k = new BigDecimal(lon2).setScale(2, BigDecimal.ROUND_HALF_UP).toString() + "," + new BigDecimal(lat2).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
				if(map.containsKey(a)){
					count++;
					calculateAvg(e,map.get(a).toString());
				}else if(map.containsKey(b)){
					count++;
					calculateAvg(e,map.get(b).toString());
				}else if(map.containsKey(c)){
					count++;
					calculateAvg(e,map.get(c).toString());
				}else if(map.containsKey(d)){
					count++;
					calculateAvg(e,map.get(d).toString());
				}else if(map.containsKey(e)){
					count++;
					calculateAvg(e,map.get(e).toString());
				}else if(map.containsKey(f)){
					count++;
					calculateAvg(e,map.get(f).toString());
				}else if(map.containsKey(g)){
					count++;
					calculateAvg(e,map.get(g).toString());
				}else if(map.containsKey(h)){
					count++;
					calculateAvg(e,map.get(h).toString());
				}else if(map.containsKey(k)){
					count++;
					calculateAvg(e,map.get(k).toString());
				}else{    //�õ���Χû���Զ�վ
					continue;
					
				}
			}
			
			System.out.println(i);
		}
		
		try {
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(count);
		System.out.println(sectionMap.size());
		//��ȡ�������ļ������ÿ�����ƽ�����ֵ
		
		if(sectionSet.size()>0){   //
			getAvg();
		}else{
			
		}
	}*/
	
	public static void calculateAvg(String point,String stationId){     //��ȡ��
//		point = "139.91,59.99";
		String lon = point.split(",")[0];
		String lat = point.split(",")[1];
		
		BigDecimal lonB = (new BigDecimal(lon)).subtract(new BigDecimal(70)).setScale(2);
		BigDecimal latB = new BigDecimal(lat).setScale(2);
		BigDecimal step = new BigDecimal(100);
		
		String lonString = lonB.multiply(step).setScale(0).toString();
		String latString = latB.multiply(step).setScale(0).toString();
		
		String section = latString + "," + lonString;
		try {
			sectionMap.put(section, stationId);
			fw.append(stationId);
			fw.append(",");
			fw.append(section);
			fw.newLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void getAvg(){   //��nc�ļ���ȡƽ��ֵ
		
		InputStream inputStream = GetPointAvgError.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String ncPath = properties.getProperty("TempNcPath");
		String obsPath = properties.getProperty("ObsPath");
		String elementName = properties.getProperty("temp");
		String outPutPath =properties.getProperty("outPutPath");
		String programPath = properties.getProperty("programPath");
		
		HashMap<String, int[]> dataMap = new HashMap<String,int[]>();//��γ�ȱ����Ϊkey���¶Ȳ�������Ϊvalue
		HashMap<String, Integer> obsMap = new HashMap<String, Integer>();//stationid+time��Ϊkey��temp��Ϊvalue
//		obsMap = new HashMap<String, Float>(1048576, 0.75f);   //����ʵ��վ�Ĵ�С����map�ĳ�ʼ����С�����putЧ�ʣ�Ĭ��ʵ��վΪ2700���Ҹ���map��ʼ����СΪ2��20���ݼ���1048576��Ĭ�ϼ�������Ϊ0.75f��
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat hourSdf = new SimpleDateFormat("yyyyMMddHH");
		Date date = new Date();
		int hour = date.getHours();
		int month = date.getMonth() + 1;   //��ȡ��ǰ�·�Ҫ + 1
		Calendar fcCal = Calendar.getInstance();
		String varName = "temp";
		int timeStep = 3;
		int hourCount = 120;
		
		//�������Ȱ�ʵ���ļ������ô���map��������洦��map��key�ĸ�ʽΪstationid+yyyyMMddHH
		/**
		 * ʵ���ļ�������ƣ��ļ�ÿ���ȡ���Σ��ֱ���00��12��00ʱ��ǰһ��23ʱ��ʵ���ļ���ʼ����ÿ3��Сʱ��һ���ļ�������ѭ����10�죬12ʱ�ӵ���11ʱ��ʵ���ļ���ʼ��
		 * 
		 */
		String day = sdf.format(date);
		Calendar obsCal = Calendar.getInstance();
		String obsBeginTimeS = null;
		String obsFilePath = null;
		String fcBeginHourS = null;
		if(hour>=0&hour<12){   //00ʱ�ο�ʼ
//		if(hour>12){
			obsCal.add(Calendar.DAY_OF_MONTH, -1);
			Date beginDay = obsCal.getTime();
			String obsBeginDayS = sdf.format(beginDay);
			String obsBeginHourS = "23";
			obsBeginTimeS = obsBeginDayS + obsBeginHourS;
			fcBeginHourS = "08";
		}else{//12ʱ�ο�ʼ
			Date obsBeginDay = date;
			String obsBeginDateS = sdf.format(obsBeginDay);
			String obsBeginHourS = "11";
			obsBeginTimeS = obsBeginDateS + obsBeginHourS;
			fcBeginHourS = "20"; 
		}
		for(int i=0;i<24*10;i+=timeStep){
			try {
				obsCal.setTime(hourSdf.parse(obsBeginTimeS));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			obsCal.add(Calendar.HOUR_OF_DAY, -i);
			Date nowDate = obsCal.getTime();
			String obsTime = hourSdf.format(nowDate);
			obsFilePath = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\obsData\\"+"obs" + obsTime + ".dat";
//			obsFilePath = obsPath+"obs" + obsTime + ".dat";
			try {
				
				File obsFile = new File(obsFilePath);
				if(obsFile.exists()){
					BufferedReader br = new BufferedReader(new FileReader(obsFile));
					String lineTxt = null;
					int lineCount =0;
					while((lineTxt = br.readLine()) != null){
//						System.out.println(lineTxt);
						String []array = lineTxt.split("\\s++");
//						if(array[0].equals("obs_time") || array[0].equals("վ��")){
						if(lineCount<2){
							lineCount++;
							continue;
						}else{
							String stationId = array[0];
							int temp;
							if(array.length>10){  //ȷ�����¶����Ҫ�أ���������Խ��
//								System.out.println(array[10]);
								temp = new BigDecimal(array[10]).multiply(new BigDecimal(10)).setScale(0).intValue();
							}else{
								temp = Integer.valueOf("999999");
							}
							String obsKey = stationId + obsTime;
							if(!obsMap.containsKey(obsKey)){
								obsMap.put(obsKey, temp);
							}
							lineCount++;
						}
					}
					br.close();
				}else{
					continue;
				}
			} catch (Exception e) {
//				e.printStackTrace();
			}
//			System.out.println(i);
		}
		System.out.println("obsMapSize-->"+obsMap.size());
		
	/********************************************************************************************************************************************************************/	
		//��ȡnc�ļ������Ҹ���ʱ����Чʵ�������Աȣ�������Ԥ��ֵ��ȥʵ��ֵ��������ȥ֮���ֵ�ĺʹ���dataMap��
		/*for(int i =1;i<=10;i++){
//			for(int i =1;i<=10;i++){
			fcCal.setTime(date);
			fcCal.add(Calendar.DAY_OF_MONTH, -i);
			Date nowDate = fcCal.getTime();
			String fcBeginDayS = sdf.format(nowDate);
			String fcBeginTimeS = fcBeginDayS + fcBeginHourS;
			
			for(int j=0;j<2;j++){   //�����¶�Ԥ��nc�ļ����ֳ���������������Ҫѭ�����������������ļ�
				String fileName = ncPath + elementName + "_" + fcBeginTimeS + "00_" + "24003" + "_" + j + ".nc";
//				String fileName = "C:\\Users\\Robin\\Desktop\\grib2\\nc\\Temperature_height_above_ground_201608050800_24003_0.nc";
				System.out.println("open file��"+fileName);
				NetcdfFile ncFile = null;
				File file = new File(fileName);
				try {
					Date fcBeginDate = hourSdf.parse(fcBeginTimeS);
					if(file.exists()){   //����ļ����ڣ�����ļ�
						System.out.println("file exist");
						ncFile = NetcdfFile.open(fileName);
						Variable v = ncFile.findVariable(varName);
						int timeCount = 0;
						if(i * 24 < hourCount){
							timeCount = i * 24 / timeStep;
						}else{
							timeCount = hourCount / timeStep;
						}
						String timeSection = "0:" + String.valueOf(timeCount - 1);
						
						if(j==1){
							Calendar midCal = Calendar.getInstance();
							midCal.setTime(fcBeginDate);
							midCal.add(Calendar.HOUR_OF_DAY, -(timeCount * timeStep));
							fcBeginTimeS = hourSdf.format(midCal.getTime());
						}
//						System.out.println(fcBeginTimeS);
						
						String stationId = null;
						String section = null;
						Array data =  null;
						IndexIterator s = null;
						int [] pointData;
						for(String key : sectionMap.keySet()){
							stationId = sectionMap.get(key).toString();
							section = timeSection + "," + key;
							data = v.read(section);
							s =  data.getIndexIterator();
							if(!dataMap.containsKey(key)){
								pointData = new int[2];
							}else{
								pointData = dataMap.get(key);
							}
							Calendar cal = Calendar.getInstance();
							int timeNum =0;
							while(s.hasNext()){
								cal.setTime(fcBeginDate);
								cal.add(Calendar.HOUR_OF_DAY, -(timeNum * timeStep));
								String time = hourSdf.format(cal.getTime());
								String obsKey = stationId + time;
								int fcData = s.getIntNext();
								if(obsMap.containsKey(obsKey)){
									int ObsData =obsMap.get(obsKey);
									if(999999==ObsData || 9999990==ObsData){
										timeNum++;
										continue;
									}else{  //���ʵ��ֵ��Ч������Ԥ����ֵ��ȥʵ����ֵ������¼�������������ż���֮�ͺ͸���
										pointData[0] += (fcData-ObsData);
										pointData[1]++;
										dataMap.put(key, pointData);
									}
								}else{
									timeNum++;
									continue;
								}
								timeNum++;
							}
						}
					}else{//���nc�ļ�������
						System.out.println("file not exist");
						continue;	
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					try {
						if(ncFile!=null){
							ncFile.close();	
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		System.out.println("�ڶ��׶�");*/
		
		for(int i =1;i<=10;i++){
			fcCal.setTime(date);
			fcCal.add(Calendar.DAY_OF_MONTH, -i);
			Date nowDate = fcCal.getTime();
			String fcBeginDayS = sdf.format(nowDate);
			String fcBeginTimeS = fcBeginDayS + fcBeginHourS;
			for(int j=0;j<2;j++){
				String fileName = ncPath + elementName + "_" + fcBeginTimeS + "00_" + "24003" + "_" + j + ".nc";
				System.out.println("open file��"+fileName);
				NetcdfFile ncFile = null;
				File file = new File(fileName);
				try {
					Date fcBeginDate = hourSdf.parse(fcBeginTimeS);
					if(file.exists()){
						System.out.println("file exist");
						ncFile = NetcdfFile.open(fileName);
						Variable v = ncFile.findVariable(varName);
						int timeCount = 0;
						if(i * 24 < hourCount){
							timeCount = i * 24 / timeStep;
						}else{
							timeCount = hourCount / timeStep;
						}
						String timeSection = "0:" + String.valueOf(timeCount - 1);
						if(j==1){
							Calendar midCal = Calendar.getInstance();
							midCal.setTime(fcBeginDate);
							midCal.add(Calendar.HOUR_OF_DAY, -(timeCount * timeStep));
							fcBeginTimeS = hourSdf.format(midCal.getTime());
						}
						
						int[] shape = v.getShape();
						String section = timeSection + "," + "0:" + String.valueOf(shape[1]-1) + "," + "0:" + String.valueOf(shape[2]-1);
						Array data = v.read(section);
						short[][][] tempCorrection = (short[][][]) data.copyToNDJavaArray();
						v=null;
						data = null;
						ncFile.close();
						String stationId = null;
						int [] pointData;
						
						for(String key : sectionMap.keySet()){
							stationId = sectionMap.get(key).toString();
							if(!dataMap.containsKey(key)){
								pointData = new int[2];
							}else{
								pointData = dataMap.get(key);
							}
							Calendar cal = Calendar.getInstance();
							for(int k = 0;k<tempCorrection.length;k++){
								cal.setTime(fcBeginDate);
								cal.add(Calendar.HOUR_OF_DAY, -(k * timeStep));
								String time = hourSdf.format(cal.getTime());
								String obsKey = stationId + time;
								int lat = Integer.valueOf(key.split(",")[0]);
								int lon = Integer.valueOf(key.split(",")[1]);
								int fcData = tempCorrection[k][lat][lon];
								if(obsMap.containsKey(obsKey)){
									int ObsData =obsMap.get(obsKey);
									if(999999==ObsData || 9999990==ObsData || ObsData>500 || ObsData<-700 ||fcData>500 || fcData<-700 ){
										continue;
									}else{
										pointData[0] += (fcData-ObsData);
										pointData[1]++;
										dataMap.put(key, pointData);
									}
								}else{
									continue;
								}
							}
						}
					}else{
						System.out.println("file not exist");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} 
		sectionMap = null;    //��Map����Ϊnull���ͷ��ڴ�
		obsMap = null;
		
		/*****************************************************************************************************************************************************************/
		//���������ļ������е���ƽ��ֵ,�����ջ��ֺõ�������ɲ�������Ȼ��ȡ�ź����80%λ�õ���
		
		HashMap<String, ArrayList<Float>> avgMap = new HashMap<String, ArrayList<Float>>();
		HashMap<String, Float> pointAvgMap = new HashMap<String, Float>();
		
		System.out.println("dataMapSize-->"+dataMap.size());
		for(String point : dataMap.keySet()){   //���ݾ�γ�ȷ���Ϊ������
			int[] array = dataMap.get(point);
			int totalTemp = array[0];
			int totalNum = array[1];
			float avgTemp = totalTemp / totalNum;
//			System.out.println(avgTemp);
			pointAvgMap.put(point, avgTemp);
			
			int lat = Integer.valueOf(point.split(",")[0]);
			int lon = Integer.valueOf(point.split(",")[1]);
			
			String gridKey = getGridKey(lat,lon);
			if(avgMap.containsKey(gridKey)){
				ArrayList<Float> avgList = avgMap.get(gridKey);
				avgList.add(avgTemp);
				avgMap.put(gridKey, avgList);
			}else{
				ArrayList<Float> avgList = new ArrayList<Float>();
				avgList.add(avgTemp);
				avgMap.put(gridKey, avgList);
			}
		}
		
		dataMap = null;   
		
		errorMap = new HashMap<String, Float>();
		for(String gridKey : avgMap.keySet()){
			ArrayList<Float> list = avgMap.get(gridKey);
			Object [] array = list.toArray();
			Arrays.sort(array);
			int index = 0;
			float minData = Float.valueOf(array[0].toString());   //ȡ��Сֵ
			float maxData = Float.valueOf(array[array.length-1].toString());  //���ֵ
			float errorData = 0;
			if(minData>=0){
				index = (int) (array.length * 0.2);
				errorData = Float.valueOf(array[index].toString());
			}else if(maxData<=0){
				index = (int) (array.length * 0.8);
				errorData = Float.valueOf(array[index].toString());
			}else{
				errorData = 0;
			}
			
			/*if(minData<0&data80<0 || minData>0&data80>0){
				errorData = data80;
			}else{
				errorData = 0;
			}*/
			errorMap.put(gridKey, errorData);
		}
		
		avgMap = null;
		System.out.println("errorMapSize-->"+errorMap.size());
		
		/******************************************��ʼдnetcdf�ļ�***********************************************************/
		/*BufferedReader br = null;
		String max_station_path = programPath + "tmax_extreme_station.dat";
		String min_station_path = programPath + "tmin_extreme_station.dat";
		String lineTxt = null;
		HashMap<String, String> maxTempMap = new HashMap<String, String>();
		HashMap<String, String> minTempMap = new HashMap<String, String>();
		
		try {
			br = new BufferedReader(new FileReader(max_station_path));
			int index = month + 2;
			while((lineTxt = br.readLine()) != null){
				String []array = lineTxt.split("\\s++");
				if(index > array.length-1){
					continue;
				}else{
					String lat = array[1];
					String lon = array[2];
					String lonB = new BigDecimal(lon).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
					String latB = new BigDecimal(lat).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
					String key = latB + "," + lonB;
					String maxTemp = array[index].trim();
					maxTempMap.put(key, maxTemp);
				}
			}
			br = null;
			lineTxt = null;
			br = new BufferedReader(new FileReader(min_station_path));
			while((lineTxt = br.readLine()) != null){
				String []array = lineTxt.split("\\s++");
				if(index > array.length-1){
					continue;
				}else{
					String lat = array[1];
					String lon = array[2];
					String lonB = new BigDecimal(lon).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
					String latB = new BigDecimal(lat).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
					String key = latB + "," + lonB;
					String minTemp = array[index].trim();
					minTempMap.put(key, minTemp);
				}
			}
			br.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}*/
		
		
//		��errorMap�е�ֵд��nc�ļ��У�ѭ����γ�ȷ�ΧΪ��0:60N,70:140E��Χ�ڵĵ㣬����Ϊ0.01��
		String filename = outPutPath + elementName + "_" + day + fcBeginHourS+ ".nc";
		System.out.println("correction netcdf file name-->"+filename);
//		String filename = "avgError1.nc";
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//��������д���ļ������ٶ����
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
	    try {
			dataFile = NetcdfFileWriter.createNew(version, filename,chunker);
			Dimension xDim = dataFile.addDimension(null, "lat", latGrid);
	        Dimension yDim = dataFile.addDimension(null, "lon", lonGrid);
	        Dimension zDim = dataFile.addDimension(null, "time", 1);
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
	        
	        Variable dataV = dataFile.addVariable(null, "temp", DataType.FLOAT,dims_Element);
	        Variable latV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
	        Variable lonV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
	        Variable timeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
	        float[] latng = new float[6001];
	        float lat = 0.01f;
	        for(int i =0;i<6001;i++){
	        	latng[i] = lat;
	        	lat+=0.01f;
	        }
	        float[] lonng = new float[7001];
	        float lon = 70.01f;
	        for(int i =0;i<7001;i++){
	        	lonng[i] = lon;
	        	lon+=0.01f;
	        }
	        
	        dataFile.create();
	        float[]gg = new float[6001*7001];
	        for(int i=0;i<latGrid;i++){
	        	for(int j=0;j<lonGrid;j++){
	        		float data;
	        		String key = String.valueOf(i)+","+String.valueOf(j);
	        		if(pointAvgMap.containsKey(key)){    //�����Χ1km�����Զ�վ
	        			data = pointAvgMap.get(key);
//	        			System.out.println("���Զ�վ");
	        		}else{
//	        			System.out.println("û���Զ�վ");
	        			String gridKey = getGridKey(i,j);
	        			if(errorMap.containsKey(gridKey)){
	        				data = errorMap.get(gridKey);
	        			}else{
	        				data = 0f;
	        			}
	        		}
	        		gg[7001*i+j] = data;
	        	}
	        }
	        Array dataArray  = Array.factory(DataType.FLOAT, new int[]{1,latGrid,lonGrid},gg);
	        dataFile.write(dataV, dataArray);
	        dataFile.write(latV, Array.factory(latng));
			dataFile.write(lonV, Array.factory(lonng));
			dataFile.write(timeV, Array.factory(new double[]{1}));
	      
	        System.out.println("д�ļ��ɹ�");
			dataFile.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getGridKey(int lat, int lon){
		String latS = null;
		String lonS = null;
		if(lat>=1200&lat<1600){
			latS = "D";
		}else if(lat>=1600&lat<2000){
			latS = "E";
		}else if(lat>=2000&lat<2400){
			latS = "F";
		}else if(lat>=2400&lat<2800){
			latS = "G";
		}else if(lat>=2800&lat<3200){
			latS = "H";
		}else if(lat>=3200&lat<3600){
			latS = "I";
		}else if(lat>=3600&lat<4000){
			latS = "J";
		}else if(lat>=4000&lat<4400){
			latS = "K";
		}else if(lat>=4400&lat<4800){
			latS = "L";
		}else if(lat>=4800&lat<5200){
			latS = "M";
		}else if(lat>=5200&lat<5600){
			latS = "N";
		}else{
			latS = "out";
		}
		
		if(lon>=200&lon<800){
			lonS = "43";
		}else if(lon>=800&lon<1400){
			lonS = "44";
		}else if(lon>=1400&lon<2000){
			lonS = "45";
		}else if(lon>=2000&lon<2600){
			lonS = "46";
		}else if(lon>=2600&lon<3200){
			lonS = "47";
		}else if(lon>=3200&lon<3800){
			lonS = "48";
		}else if(lon>=3800&lon<4400){
			lonS = "49";
		}else if(lon>=4400&lon<5000){
			lonS = "50";
		}else if(lon>=5000&lon<5600){
			lonS = "51";
		}else if(lon>=5600&lon<6200){
			lonS = "52";
		}else if(lon>=6200&lon<6800){
			lonS = "53";
		}else{
			lonS = "out";
		}
		String gridKey = latS+lonS;
		
		return gridKey;
	}
	
	
}
