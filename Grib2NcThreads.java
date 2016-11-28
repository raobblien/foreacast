package weather.nmc.fc.threehour;

import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.LatLonRect;


public class Grib2NcThreads implements Runnable{

	static Logger logger = LogManager.getLogger(Grib2NcThreads.class.getName());
	
	private static String dataType = "nmc"; //��������
	
	private static String timeDes = "3h"; //ʱ������
	
	
	
	
	
	private static float[] lat = null;  //grib2�ļ�γ��
	
	private static float[] lon = null;   //grib2�ļ�����
	
	private static float[] latng = null;//��ֵ���γ��
	
	private static float[] lonng = null;//��ֵ��ľ���
	
	private static double[] time = null;//ʱ��
	
	private static float[][][] source = null;//grib2�ļ�������
	
	private static float[][][][] source2 = null;   //�������������ļ�ʱ����������Ҫ�أ�������Ҫ�õ����source2
	
	private static float[][][][] source3 = null; //���ʪ��Ϊ4ά���飬�������ʪ�ȵ�ʱ��Ҫ�����source3,���з�������
	
	private static int lonRange = 0;//��ֵ���γ�ȷ�Χ
	
	private static int latRange = 0;//��ֵ��ľ��ȷ�Χ
	
	private static int timeRange = 0;//ʱ�䷶Χ
	
	private static int sourceLatRange = 0;
	
	private static int sourceLonRange = 0;
	
	private int ratio = 5;//��ֵ����
	
	private float startLat = 0.0F;
	
	private float startLon = 70.0F;
			
	private float endLat = 60.0F;
	
	private float endLon = 140.0F;	
	
	private float latLonStep = 0.01F;
	
	private static float[][][] tempCorrection = null;
	
	private static String FcType;
	
	private static int timeNum;  //��ǰ�ļ���
	
	private static int ThreadCount = 10;//�߳���
	
	private static int fileCount; //�ļ�����
	
	private static int fileTimeCount; //һ���ļ���ʱ����
	
	private static byte[] ee;   //byte���飬�������ʪ�ȡ�����
	
	private static short[] gg,wsd,wdr;   //short���飬�����¶ȡ��硢��ˮ
	
	private static ConcurrentLinkedQueue<Integer> queue = new ConcurrentLinkedQueue<Integer>();
	
	private static Set<Integer> set = new HashSet<Integer>();
	
	/**
	 * @author Robin
	 * @param args[0]:date ʱ��
	 * @param args[1]:element Ҫ��
	 * describe:��nmc5Km���¶ȡ���ˮ�������ʪ�ȡ��硢������Ҫ���ļ�ͨ����ֵ���ʿصȷ���ת����1Km���nc�ļ�
	 */
	public static void main(String[] args) {
		
		Grib2NcThreads gnt = new Grib2NcThreads();
		FcType = args[0];
//		String FcType = "r03";
		Date date;
		SimpleDateFormat sdfh = new SimpleDateFormat("yyyyMMddHH");
		try {
			if(args.length>1){//�����ʱ����������ô����ʱ���������û����Ĭ�ϵ�ǰʱ��Ϊʱ�����
				date = sdfh.parse(args[1]);
			}else{
				date = new Date();
			}
			DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String time1=format.format(date);
			System.out.println("��ʼʱ�䣺"+time1);
			logger.error(FcType+"-->��ʼʱ�䣺"+time1);
			long begin = System.currentTimeMillis(); 
			
			/**********************��ȡ�����ļ���Ϣbegin**************************/
			InputStream inputStream = Grib2NcThreads.class.getClassLoader().getResourceAsStream("configs/pro.properties");
		 	Properties properties = new Properties();
			properties.load(inputStream);
			String pro = properties.getProperty(FcType);     //��ȡҪ���������ƣ�������Ҫ��������Ϊ�������ɵ�nc�ļ�������
			String gribFilePath = properties.getProperty("gribFilePath"); //��ȡGrib�ļ�·��
			String fcTime = properties.getProperty("fcTime");  //��ȡԤ��ʱ��
			String[] fcTimeArray = fcTime.split(","); //Ԥ��ʱ������
			fileCount = Integer.valueOf(fcTimeArray[0]);   //���ɵ�nc�ļ�����
			fileTimeCount = Integer.valueOf(fcTimeArray[1]); //ÿ���ļ���ʱ����
			String element = null;
			String elementU = null;
			String elementV = null;
			String fileNameHead = null;
			String timeStep = null;
			String elementName = null;
			String corFilePath = null;
			if("wind".equals(FcType)){
				elementU = pro.split(",")[1];//grib2�ļ�Ҫ������
				elementV = pro.split(",")[2];
				fileNameHead = pro.split(",")[0];//�ļ���ͷ����
				timeStep = pro.split(",")[3];   //�ļ���ʱ�䲽��
				elementName = pro.split(",")[4];    //дnc�ļ���Ҫ������
				element = "wind";
			}else{
				fileNameHead = pro.split(",")[0];//�ļ���ͷ����
				element = pro.split(",")[1].trim();//grib2�ļ�Ҫ������
				timeStep = pro.split(",")[2].trim();   //�ļ���ʱ�䲽��
				elementName = pro.split(",")[3].trim();    //дnc�ļ���Ҫ������
				if("temp".equals(FcType)){
					corFilePath = pro.split(",")[4].trim();
				}
			}
			String outPutPath = properties.getProperty("outPutPath");//���Ŀ��·��
			/*******************��ȡ�����ļ���Ϣend*******************************/
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			String fileDay = sdf.format(date);
			int hour = date.getHours();
			String fileDate = null;
			if(hour>=12){      //�жϣ������ǰʱ��Ϊ12����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
				fileDate = fileDay+"2000"+timeStep;
			}else{
				fileDate = fileDay+"0800"+timeStep;
			}	
			//��ȡԭʼGrib�ļ����������ݼ��ص��ڴ���
			if("wind".equals(FcType)){
				gnt.ReadGrib2WindFile(date,elementU,elementV,fileNameHead,timeStep,elementName,outPutPath,gribFilePath);
			}else{
				gnt.ReadGrib2File(date,element,fileNameHead,timeStep,elementName,outPutPath,gribFilePath,corFilePath);
			}
			
			for(timeNum = 0;timeNum<fileCount;timeNum++){
				if("wind".equals(FcType)){
					wsd = new short[fileTimeCount * (latRange) * (lonRange)];
					wdr = new short[fileTimeCount * (latRange) * (lonRange)];
				}else if("cloud".equals(FcType) || "rh".equals(FcType)){
					ee = new byte[fileTimeCount * (latRange) * (lonRange)];
				}else{
					gg = new short[fileTimeCount * (latRange) * (lonRange)];
				}
				set.clear();
				for(int j = timeNum * fileTimeCount;j<(timeNum + 1) * fileTimeCount;j++){ //��ʱ�η����̰߳�ȫ�����ж�����
					queue.add(j);
				}
				
				ExecutorService exe = Executors.newFixedThreadPool(ThreadCount);
				for (int i = 0; i < ThreadCount; i++){
					exe.execute(gnt);    
		        }
				exe.shutdown();
				while (true) {
					if (exe.isTerminated()) {
						if("wind".equals(FcType)){
							gnt.WriteWindNcFile(outPutPath,elementName,element,fileDate,fileTimeCount);
						}else{
							gnt.WriteNcFile(outPutPath,elementName,element,fileDate,fileTimeCount);
						}
						System.gc();
						break;
					}
				}
			}
			
			long end = System.currentTimeMillis();
			Date date1=new Date();
			String time2=format.format(date1);
			System.out.println("����ʱ�䣺"+time2);
			System.out.println("����ʱ�䣺"+(end-begin)+"ms");
			logger.error(FcType + "-->����ʱ�䣺"+time2);
			logger.error(FcType + "-->����ʱ�䣺"+(end-begin)+"ms");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		Grib2NcThreads gnt = new Grib2NcThreads();
		try {
			if("wind".equals(FcType)){
				gnt.WindDataCal();
			}else{
				gnt.DataCal();
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(FcType+"-->"+e);
		}
	}
	
	/**
	 * 
	 * ���̲߳�ֵ����
	 * @throws Exception
	 */
	public void DataCal()throws Exception{   
		int lat_min,lat_max,lon_min,lon_max;
	 	while(!queue.isEmpty()){
			int time = queue.poll();
			set.add(time);
			System.out.println(time);
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
						byte data = DoubleLineInter3(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  
						ee[(time - timeNum * fileTimeCount) * latRange * lonRange + j * lonRange + k] = data;
					}else{
						short data = DoubleLineInter(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  
						gg[(time - timeNum * fileTimeCount) * latRange * lonRange + j * lonRange + k] = data;
					}
				}
			}
		}
	}
	
	public void WindDataCal() throws Exception{  //���̼߳����������
		int lat_min,lat_max,lon_min,lon_max;
	 	while(!queue.isEmpty()){
	 		int time = queue.poll();
			set.add(time);
			System.out.println(time);
			
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
					float wind_u = DoubleLineInter2(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"u");  //ͨ����ֵ��ȡu������ֵ
					float wind_v = DoubleLineInter2(time, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"v");  //ͨ����ֵ��ȡv������ֵ
					wsd[(time - timeNum * fileTimeCount) * latRange * lonRange + j * lonRange + k] = (short) wind_u;
					wdr[(time - timeNum * fileTimeCount) * latRange * lonRange + j * lonRange + k] = (short) wind_v;
				}
			}
	 	}
		
	}
	
	/**
	 * ��ȡԭGrib�ļ����������ݼ��ص��ڴ���
	 * @param date
	 * @param element
	 * @param fileNameHead
	 * @param timeStep
	 * @param elementName
	 * @param outPutPath
	 * @param filePath
	 * @param corFilePath
	 * @throws Exception
	 */
	public void ReadGrib2File(Date date,String element,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath,String corFilePath)throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		String beginHours = null;
		
		if(hour>=12){      //�жϣ������ǰʱ��Ϊ18����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
			beginHours = "20";
			fileDate = fileDay+"2000"+timeStep;
		}else{
			beginHours = "08";
			fileDate = fileDay+"0800"+timeStep;
		}	
		
		//���Ϊ�¶�Ԥ��������Ҫ�Ƚ����¶�Ԥ���Ķ����ļ�
		
		if(FcType.indexOf("temp")>=0){
//			String corFilePath = pro.split(",")[4].trim();
			String tempFileName = corFilePath + element + "_" + fileDay + beginHours + ".nc" ;
			File file = new File(tempFileName);
			System.out.println("open correction file -->" + tempFileName);
			if(file.exists()){
				NetcdfFile CorrectionFile = null;   //�¶ȶ���nc�ļ�
				Variable CorrectionV = null;
				CorrectionFile = NetcdfFile.open(tempFileName);
				System.out.println("file exist and open success");
				CorrectionV = CorrectionFile.findVariable(elementName);
				int[] shape = CorrectionV.getShape();
				String section = "0," + "0:" + String.valueOf(shape[1]-1) + "," + "0:" + String.valueOf(shape[2]-1);
//				System.out.println(section);
				Array data = CorrectionV.read(section);
				tempCorrection = (float[][][]) data.copyToNDJavaArray();  //�����ڴ�
				System.out.println(tempCorrection[0].length);
				System.out.println(tempCorrection[0][0].length);
				CorrectionFile.close();   //�ر��ļ�
			}else{
				System.out.println("correction file not exist");
			}
		}
/*********************************************����ԭʼGRIB2�ļ�begin*******************************************************************/
		
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //�ļ�������ע�͵���ҵ�񻯺��ٴ�
		
//		String fileName = "Z_NWGD_C_BABJ_P_RFFC_SCMOC-ER03_201609120800_24003.GRB2";
		
		GridDataset gds = ucar.nc2.dt.grid.GridDataset.open(fileName);
		
		GridDatatype grid = gds.findGridDatatype(element);
		GridDatatype subGrid = grid.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		
		lat = (float[])gds.getNetcdfDataset().findVariable("lat").read().copyTo1DJavaArray();//ԭʼ�ļ�γ��
		
		sourceLatRange = subGrid.getDimension(subGrid.getYDimensionIndex()).getLength();
		
		LatLonRect latLonRect = gds.getBoundingBox();
		
		latLonStep = (float)(Math.round((latLonRect.getWidth())/(lat.length - 1)/ratio*100))/100;  //����
		
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
		
		//��ȡԭʼ����
		if("rh".equals(FcType)||"temp".equals(FcType)){
			source3 = (float[][][][])subGrid.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		}else{
			source = (float[][][])subGrid.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		}
		gds.close();
	}
	
	/**
	 * 
	 * ��ȡ��Ҫ�ص�ԭGrib�ļ�
	 * @param date
	 * @param elementU
	 * @param elementV
	 * @param fileNameHead
	 * @param timeStep
	 * @param elementName
	 * @param outPutPath
	 * @param filePath
	 * @throws Exception
	 */
	public void ReadGrib2WindFile(Date date,String elementU,String elementV,String fileNameHead,String timeStep,String elementName,String outPutPath,String filePath)throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		if(hour>=12){      //�жϣ������ǰʱ��Ϊ18����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
			fileDate = fileDay+"2000"+timeStep;
		}else{
			fileDate = fileDay+"0800"+timeStep;
		}
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //�ļ�������ע�͵���ҵ�񻯺��ٴ�
//		String fileName = "Z_NWGD_C_BABJ_20161009201616_P_RFFC_SCMOC-EDA10_201610100800_24003.GRB2";
		GridDataset gds = ucar.nc2.dt.grid.GridDataset.open(fileName);
		GridDatatype gridU = gds.findGridDatatype(elementU);
		GridDatatype gridV = gds.findGridDatatype(elementV);
		GridDatatype subGridU = gridU.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		GridDatatype subGridV = gridV.makeSubset(null, null, new LatLonRect(new LatLonPointImpl(startLat,startLon),new LatLonPointImpl(endLat,endLon)), 1, 1, 1);
		lat = (float[])gds.getNetcdfDataset().findVariable("lat").read().copyTo1DJavaArray();//ԭʼ�ļ�γ��
		sourceLatRange = subGridU.getDimension(subGridU.getYDimensionIndex()).getLength();
		LatLonRect latLonRect = gds.getBoundingBox();
		latLonStep = (float)(Math.round((latLonRect.getWidth())/(lat.length - 1)/ratio*100))/100;  //����
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
		//��ȡԭʼ����
		source2 = (float[][][][])subGridU.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		source3 = (float[][][][])subGridV.readDataSlice(-1, -1, -1, -1).copyToNDJavaArray();
		gds.close();
	}
	
	/**
	 * 
	 * дnc�ļ�
	 * @param outPutPath
	 * @param elementName
	 * @param element
	 * @param fileDate
	 * @param realTime
	 * @throws Exception
	 */
	public void WriteNcFile(String outPutPath,String elementName,String element,String fileDate,int realTime)throws Exception{
		String filename = outPutPath + elementName +"/" + element+ "_" + fileDate + "_" + timeNum + ".nc";
//		String filename = outPutPath + 
		
//		String filename = elementName + "_" + timeNum + ".nc";
		System.out.println("netcdf out put path-->"+filename);
		NetcdfFileWriter dataFile = null;
	    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//��������д���ļ������ٶ����
		NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
	    dataFile = NetcdfFileWriter.createNew(version, filename,chunker);
    	Dimension xDim = dataFile.addDimension(null, "lat", latRange);
        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
        Dimension zDim = dataFile.addDimension(null, "time", realTime);  //Ĭ�����Сʱ�ļ���ÿ���ļ�ʱ��Ϊ48��ʱ��
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
		if("rh".equals(FcType) || "cloud".equals(FcType)){    //���ʪ�ȷ�ΧΪ0-100����byte���ʹ洢
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
    		timeData[index] = (time + 1) * 3;
        	index++;
        }
        
        Arrays.sort(timeData);
		Array timeArray = Array.factory(DataType.DOUBLE, new int[]{realTime},timeData);
		dataFile.write(timeV, timeArray);
		dataFile.write(latV, Array.factory(latng));
		dataFile.write(lonV, Array.factory(lonng));
		System.out.println("write netcdf success!!!");
		dataFile.close();
	}
	
	/**
	 * 
	 * д��nc�ļ�
	 * @param outPutPath
	 * @param elementName
	 * @param element
	 * @param fileDate
	 * @param realTime
	 * @throws Exception
	 */
	public void WriteWindNcFile(String outPutPath,String elementName,String element,String fileDate,int realTime)throws Exception{ //д���������nc�ļ�
		
		String filename = outPutPath + elementName +"/" + element+ "_" + fileDate + "_" + timeNum + ".nc";
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
    		timeData[index] = (time + 1) * 3;
        	index++;
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
	
	/**
	 * 
	 * ��ˮ�����ݼ�ֵУ��
	 * @param data ��ˮ��
	 * @return data
	 */
	public float ValidData(float data){   
		return data>999 ? 0.0F : data;
	}
	
	/**
	 * 
	 * �¶����ݼ�ֵУ��
	 * @param data �¶�
	 * @return data
	 */
	public float ValidTempData(float data){   
		if(data==9999){//���¶�Ϊ0�����¶�<-50,>50��ʱ��Ҳ��ȡ��һ�ε��ļ�
			data = 0;        //��ʱ�ȶ�Ϊ0���Ժ�������̻��ȡ��һ��ʱ�ε��ļ������Ǻ���γ�ȸ���Ե�����ݶ���9999.0
		}else{
			data = data - 273.15f;
		}
		return data;
	}
	
	/**
	 * 
	 * ���ʪ�ȼ�ֵУ��
	 * @param data
	 * @return data
	 */
	public float ValidRhData(float data){   //���ʪ�����ݼ�ֵУ��
		return data>100 || data<0 ? 0.0F : data;
	}
	
	/**
	 * ˫���Բ�ֵ�㷨
	 * @param time
	 * @param lat_min
	 * @param lat_max
	 * @param lon_min
	 * @param lon_max
	 * @param lat_1Km
	 * @param lon_1Km
	 * @param lat_5Km_min
	 * @param lat_5Km_max
	 * @param lon_5Km_min
	 * @param lon_5Km_max
	 * @param type
	 * @return
	 * @throws Exception
	 */
	public short DoubleLineInter(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String type) throws Exception{   //˫���Բ�ֵ�㷨
		float fx = lat_5Km_max - lat_5Km_min;
		float Q11;
		float Q21;
		float Q12;
		float Q22;
		float CorTemp = 0;
		
		if(type.indexOf("temp")>=0){    //���Ԥ������Ϊ�¶ȣ���ʹ���¶ȵļ�ֵ����
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
	
	//˫���Բ�ֵ������������Ҫ��source2����ȡֵ��ֻΪ�������������ļ�
	public float DoubleLineInter2(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String wind){   //˫���Բ�ֵ�㷨
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
	
	//���ʪ��˫���Բ�ֵ����������Ϊbyte
	public byte DoubleLineInter3(int time ,int lat_min,int lat_max,int lon_min,int lon_max,float lat_1Km,float lon_1Km,float lat_5Km_min,float lat_5Km_max,float lon_5Km_min,float lon_5Km_max,String type) throws Exception{   //˫���Բ�ֵ�㷨
		float fx = lat_5Km_max - lat_5Km_min;
		float Q11;
		float Q21;
		float Q12;
		float Q22;
		
		if("cloud".equals(FcType)){   //����Ҫ����source
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
