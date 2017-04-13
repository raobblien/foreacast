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
	private static String dataType = "nmc"; //��������
	private static String timeDes = "1h"; //ʱ������
	private static int Offset = 0; //ƫ������Ĭ��Ϊ0
	private float startLat = 0.0F;
	private float startLon = 70.0F;
	private float endLat = 60.0F;
	private float endLon = 140.0F;	
	private float latLonStep = 0.01F;
	private static int dayNum; //����
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
	 * ���߳�д��������nc�ļ�
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
		System.out.println("��ʼʱ�䣺"+time1);
		logger.error("weatherCode-->��ʼʱ�䣺"+time1);
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
		String popPath = pro.split(",")[0]; //��ˮ��nc�ļ�path
		String tempPath = pro.split(",")[1]; //�¶�nc�ļ�path
		String cloudPath = pro.split(",")[2]; //����nc�ļ�path
		String outPath = properties.getProperty("OutPutPath_1h");
		String ocfPath = properties.getProperty("ocfPath");
		String ocfHead = properties.getProperty("ocfFileHead");
		String ocfEnd = properties.getProperty("ocfFileEnd");
		String disFile = properties.getProperty("disFile");
		
		String fileHour = null;
		if(hour>3&hour<12){  //�����ǰСʱΪ3-12�㣬����Ϊ��ˮ�����¶ȵ�Ԥ���ļ�Ϊ08��������ΪԤ���ļ�Ϊ20
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
				if(!flag){//����ļ���ͬʱ����
					continue;
				}
				System.gc();
				for(int j = 0;j < timeRange;j++){ //��ʱ�η��������   ,
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
		logger.error("weatherCode-->����ʱ�䣺"+time2);
		logger.error("weatherCode-->����ʱ�䣺"+(end-begin)+"ms");
		System.out.println("����ʱ�䣺"+time2);
		System.out.println("����ʱ�䣺"+(end-begin)+"ms");
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
					if(pop>0){  //  �н�ˮ,���¶�
//						System.out.println(pop);
						short temp = (short) (tempSource[time][i][k] / 10);
						if(temp <= -2){  //ѩ
							if(pop <= 0.2){  //Сѩ
								code = 14;
							}else if(pop > 0.2 & pop <= 0.5){ //��ѩ
								code = 15;
							}else if(pop > 0.5 & pop <= 1.3){ //��ѩ
								code = 16;
							}else{ //��ѩ
								code = 17;
							}
						}else if(temp <= 2 & temp > -2){  //���ѩ
							code = 6;
						}else{//��
							if(pop<=1){  //С��
								code = 7;
							}else if(pop > 1 & pop <= 3.3){ //����
								code = 8;
							}else if(pop > 3.3 & pop <= 6.6){ //����
								code = 9;
							}else if(pop > 6.6 & pop <= 16.6){ //����
								code = 10;
							}else if(pop > 16.6 & pop <= 33.3){ //����
								code = 11;
							}else{  //�ش���
								code = 12;
							}
						}                                                                                                
					}else{  //�޽�ˮ��������
//						float cloud = new BigDecimal(cloudSource[t][i][k]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
//						float cloud = (Float.valueOf(String.valueOf(cloudSource[t][i][k]))) / 10;
						float c = Float.valueOf(String.valueOf(cloudSource[time][i][k]));
						float cloud=(float) (Math.round(c*10)/10.0);
						if(cloud <= 20){ //����
							code = 0;
						}else if(cloud > 20 & cloud <= 80){ //����
							code = 1;
						}else{ //����
							code = 2;
						}
					}
//					System.out.println(code);
					//վ���ں�
					
					
					int stationId = stationIdSource[0][i][k];
					String idStr = String.valueOf(stationId);
					double distance = 999;
					if(stationId > 0){
						distance = distanceSource[0][i][k];
						if(distance <= 3){
							if(ocfMap.containsKey(idStr)){
								byte ocfCode = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue();
//								if((ocfCode>=0 & ocfCode<=31) || ocfCode == 53 || ocfCode == 301 || ocfCode == 302){    //modified by robin 2017-1-18   ע��ocf��Ԥ��
									if((ocfCode>=0 & ocfCode<=31) || ocfCode == 301 || ocfCode == 302){
									code = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue(); //ocfԤ���ӵ�һ��ʱ��Ϊ07-08������Ҫ+1
									short rain = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(0)* 10).shortValue();
									popSource[time][i][k] = rain;
								}
							}
						}else{
							if(ocfMap.containsKey(idStr)){
								byte ocfCode = new BigDecimal(ocfMap.get(idStr).get(time + nowDay * 24 + 1).get(1)).byteValue();
								//modified by robin 2017-1-18   ע��ocf��Ԥ��
								/*if(ocfCode == 53){  //ocfԤ��Ϊ��
									code = ocfCode;
									popSource[time][i][k] = 0;
								}
								else if(ocfCode == 18){ //ocfԤ��Ϊ��*/
								if(ocfCode == 18){
									if(distance < 20){
										code = ocfCode;
										popSource[time][i][k] = 0;
									}
								}else if(ocfCode == 29){  //ocfԤ��Ϊɳ��
									if(distance <= 10){
										code = ocfCode;
										popSource[time][i][k] = 0;
									}
								}else if(ocfCode == 19){  //ocfԤ��Ϊ����    add by robin 2017-1-20  ��Ӷ��궩��
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
		if(!popFile.exists() || !tempFile.exists() || !cloudFile.exists()){   //����һ���ļ�������
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
		filenameSB.append(outPath)  //·��
		.append(elementName)           //Ҫ��·��
		.append("/")          
		.append(dataType)//��������
		.append("_")
		.append(timeDes)//ʱ������
		.append("_")
		.append(elementName)//Ҫ������
		.append("_")
		.append(24 * nowDay + 1)//��ʼʱ��
		.append("_")
		.append(0)//z��ʼ
		.append("_")
		.append(new BigDecimal(startLat).setScale(0))//γ����ʼ
		.append("_")
		.append(new BigDecimal(startLon).setScale(0))//������ʼ
		.append("_")
		.append(1)//ʱ����
		.append("_")
		.append(0)//z���
		.append("_")
		.append(latLonStep)//γ�ȼ��
		.append("_")
		.append(latLonStep)//���ȼ��
		.append("_")
		.append((nowDay + 1) * 24)//ʱ�����
		.append("_")
		.append(0)//z����
		.append("_")
		.append(new BigDecimal(endLat).setScale(0))//γ�Ƚ���
		.append("_")
		.append(new BigDecimal(endLon).setScale(0))//���Ƚ���
		.append("_")
		.append(Offset)//�洢����
		.append("_")
		.append(fileDate)//ʱ��
		.append(".nc");
		System.out.println("netcdf out put path-->" + filenameSB.toString());
		
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//��������д���ļ������ٶ����
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
        
        System.out.println("��ʼд�ļ�");
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
//		.append(timeDes)//ʱ������
//		.append("_")
//		.append(elementName)//Ҫ������
//		.append("_")
		.append(24 * nowDay + 1)//��ʼʱ��
		.append("_")
		.append(0)//z��ʼ
		.append("_")
		.append(new BigDecimal(startLat).setScale(0))//γ����ʼ
		.append("_")
		.append(new BigDecimal(startLon).setScale(0))//������ʼ
		.append("_")
		.append(1)//ʱ����
		.append("_")
		.append(0)//z���
		.append("_")
		.append(latLonStep)//γ�ȼ��
		.append("_")
		.append(latLonStep)//���ȼ��
		.append("_")
		.append((nowDay + 1) * 24)//ʱ�����
		.append("_")
		.append(0)//z����
		.append("_")
		.append(new BigDecimal(endLat).setScale(0))//γ�Ƚ���
		.append("_")
		.append(new BigDecimal(endLon).setScale(0))//���Ƚ���
		.append("_")
		.append(offset)//�洢����
		.append("_")
		.append(fileData)//ʱ��
		.append(".nc");
		return sb.toString();
	}
	
	/**
	 * ��ȡocfԭ�ļ�
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
				if(length<=10){//վ����
					if(id!=null){
						ocfMap.put(id, infoMap);
						infoMap = new HashMap<Integer, List<Double>>();
					}
					id = array[0];
					continue;
				}else{
					List<Double> list = new ArrayList<Double>();
					int time = Integer.valueOf(array[1]);
					if(time > 72){  //ֻ����Сʱ������
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
