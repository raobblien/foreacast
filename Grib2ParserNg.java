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
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;

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

public class Grib2ParserNg {
	
	/**
	 * 
	 * @author Robin
	 * grib2�ļ�������˫���Բ�ֵ��������nc�ļ�
	 * ͨ�����Σ�������ͬ���͵��ļ�
	 * 
	 */
	
	private float[] lat = null;  //grib2�ļ�γ��
	
	private float[] lon = null;   //grib2�ļ�����
	
	private float[] latng = null;//��ֵ���γ��
	
	private float[] lonng = null;//��ֵ��ľ���
	
	private double[] time = null;//ʱ��
	
	private float[][][] source = null;//grib2�ļ�������
	private float[][][][] source2 = null;   //�������������ļ�ʱ����������Ҫ�أ�������Ҫ�õ����source2
	private float[][][][] source3 = null; //���ʪ��Ϊ4ά���飬�������ʪ�ȵ�ʱ��Ҫ�����source3,���з�������
	
	private int lonRange = 0;//��ֵ���γ�ȷ�Χ
	
	private int latRange = 0;//��ֵ��ľ��ȷ�Χ
	
	private int timeRange = 0;//ʱ�䷶Χ
	
	private int sourceLatRange = 0;
	
	private int sourceLonRange = 0;
	
	private int ratio = 5;//��ֵ����
	
	private float startLat = 0.0F;
	
	private float startLon = 70.0F;
			
	private float endLat = 60.0F;
	
	private float endLon = 140.0F;	
	
	private float latLonStep = 0.01F;
	
	
	private float[][][] tempCorrection = null;
	
//	static String fileName = "Z_NWGD_C_BABJ_P_RFFC_SCMOC-TMP_201607190800_24003.GRB2";
	
//	static String fileName = "/home/luobing/nc/Z_NWGD_C_BABJ_P_RFFC_SCMOC-ER03_201607220800_24003.GRB2";
//	static String FcType = "r03";
	
//	static int TimeCount = 24;
	
//	Date date = new Date();
//	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
	
	public static void main(String[] args){
		Grib2ParserNg gp = new Grib2ParserNg();
		String FcType = args[0];
//		String FcType = "r03";
		try {
			if("wind".equals(FcType)){
				gp.ReadGrib2WindFile(FcType);
			}else{
				gp.ReadGrib2File(FcType);
			}
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}
	
	public void getFileDate(String outPutPath,String elementName,String element,Date date,String timeStep){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String LastFileDate = null;   //��һ��nc�ļ�������
		String NextFileDate = null;   //��һ��nc�ļ�������
		if(hour>=16){      //�жϣ������ǰʱ��Ϊ18����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
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
			String NextFileName = outPutPath + elementName +"/" + element+ "_" + NextFileDate + "_" +t + ".nc";
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
//		String filename = outPutPath + elementName +"/" + element+ "_" + fileDate + "_" + t + ".nc";
	}
	
	public void ReadGrib2File(String FcType)throws Exception{   //����grib2�ļ�,��дnetcdf�ļ�,�Ƿ�����������
		Date date=new Date();
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("��ʼʱ�䣺"+time1);
		long begin = System.currentTimeMillis(); 
		
		InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		properties.load(inputStream);
		String pro = properties.getProperty(FcType);     //��ȡҪ���������ƣ�������Ҫ��������Ϊ�������ɵ�nc�ļ�������
		String element = pro.split(",")[1].trim();//grib2�ļ�Ҫ������
		String fileNameHead = pro.split(",")[0].trim();//�ļ���ͷ����
		String timeStep = pro.split(",")[2].trim();   //�ļ���ʱ�䲽��
		String elementName = pro.split(",")[3].trim();    //дnc�ļ���Ҫ������
		String outPutPath = properties.getProperty("outPutPath");//���Ŀ��·��
		String filePath = properties.getProperty("filePath");//Grib�ļ�·��
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		String beginHours = null;
		
		if(hour>=16){      //�жϣ������ǰʱ��Ϊ18����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
			beginHours = "20";
			fileDate = fileDay+"2000"+timeStep;
		}else{
			beginHours = "08";
			fileDate = fileDay+"0800"+timeStep;
		}
		
		//���Ϊ�¶�Ԥ��������Ҫ�Ƚ����¶�Ԥ���Ķ����ļ�
		
		if(FcType.indexOf("temp")>=0){
			String corFilePath = pro.split(",")[4].trim();
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
				tempCorrection = (float[][][]) data.copyToNDJavaArray();
				System.out.println(tempCorrection[0].length);
				System.out.println(tempCorrection[0][0].length);
				CorrectionFile.close();
			}else{
				System.out.println("correction file not exist");
			}
		}
		
		/*********************************************����ԭʼGRIB2�ļ�begin*******************************************************************/
		
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //�ļ�������ע�͵���ҵ�񻯺��ٴ�
		
//		String fileName = "Z_NWGD_C_BABJ_P_RFFC_SCMOC-ER03_201608170800_24003.GRB2";
		System.out.println("begin open grib2 file -->"+fileName);
		//��ʼ��γ��
		File file = new File(fileName);
		if(!file.exists()){   //�ж�Դ�ļ��Ƿ����,���治���ڣ���ֱ�Ӹ�����һ��ʱ�����ɵ�nc�ļ�
			getFileDate(outPutPath, elementName, element, date, timeStep);
			return;
		}
		
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
		
		/**********************************************����ԭʼGRIB2�ļ�end***************************************************************/
		
		/************************************************дnc�ļ�begin********************************************************************/
		
		//дnc�ļ�
		
		int lat_min,lat_max,lon_min,lon_max;
		
		BigDecimal a = new BigDecimal(timeRange);
		BigDecimal b = new BigDecimal(latRange);
		BigDecimal c = new BigDecimal(lonRange);
		
		BigDecimal d = a.multiply(b).multiply(c);
		BigDecimal e = new BigDecimal(Integer.MAX_VALUE);
		int count =1;
		while(e.compareTo(d)<0){
			count++;
			a = a.divide(new BigDecimal(count),0,BigDecimal.ROUND_HALF_UP);
			d = a.multiply(b).multiply(c);
		}
		int realTime = a.intValue();
		int realTimeCount = timeRange / realTime;
		
		for(int t=0;t<realTimeCount;t++){
			int arrayIndex = 0;
			byte[] ee = null;
			short[]gg = null;
			
			double[] aa = new double[realTime];
			String filename = outPutPath + elementName +"/" + element+ "_" + fileDate + "_" + t + ".nc";
//			String filename = element+ "_" + fileDate + "_" + t + ".nc";
			System.out.println("netcdf out put path-->"+filename);
			NetcdfFileWriter dataFile = null;
		    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//��������д���ļ������ٶ����
			NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
		    dataFile = NetcdfFileWriter.createNew(version, filename,chunker);
	    	Dimension xDim = dataFile.addDimension(null, "lat", latRange);
	        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
	        Dimension zDim = dataFile.addDimension(null, "time", realTime);
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
			if("rh".equals(FcType)){    //���ʪ�ȷ�ΧΪ0-100����byte���ʹ洢
				ee = new byte[realTime * (latRange) * (lonRange)];
				dataV = dataFile.addVariable(null, elementName, DataType.BYTE,dims_Element);
			}else{
				gg = new short[realTime * (latRange) * (lonRange)];
				dataV = dataFile.addVariable(null, elementName, DataType.SHORT,dims_Element);
			}
	        
	        Variable latV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
	        Variable lonV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
	        Variable timeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
	        lonV.addAttribute(new Attribute("units", "degrees_east"));
	        latV.addAttribute(new Attribute("units", "degrees_north"));
	        dataFile.create();
			for(int i =t*realTime;i<realTime*(t+1);i++){
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
						if("rh".equals(FcType)){
							byte data = DoubleLineInter3(i, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  //ͨ����ֵ��ȡ��ˮ����ֵ
							ee[arrayIndex * latRange * lonRange + j * lonRange + k] = data;
						}else{
							short data = DoubleLineInter(i, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,FcType);  //ͨ����ֵ��ȡ��ˮ����ֵ
							gg[arrayIndex * latRange * lonRange + j * lonRange + k] = data;
						}
					}
				}
				
				aa[arrayIndex] = (i+1) * 3;
				arrayIndex++;
				System.out.println(i+1);
			}
			
			if("rh".equals(FcType)){
				Array dataArray  = Array.factory(DataType.BYTE, new int[]{realTime,latng.length,lonng.length},ee);
				dataFile.write(dataV, dataArray);
			}else{
				Array dataArray  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},gg);
				dataFile.write(dataV, dataArray);
			}
			Array timeArray = Array.factory(DataType.DOUBLE, new int[]{realTime},aa);
			System.out.println("��ʼд�ļ�");
			
//				dataFile.write(timeV, Array.factory(time));
			dataFile.write(timeV, timeArray);
			dataFile.write(latV, Array.factory(latng));
			dataFile.write(lonV, Array.factory(lonng));
			System.out.println("write netcdf success!!!");
			dataFile.close();
		}
		gds.close();
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		System.out.println("����ʱ�䣺"+time2);
		System.out.println("����ʱ�䣺"+(end-begin)+"ms");
		
		/***************************************************дnc�ļ�end*************************************************************/
	}
	
	public void ReadGrib2WindFile(String FcType)throws Exception{
		Date date=new Date();
		InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		properties.load(inputStream);
		String pro = properties.getProperty(FcType);     //��ȡҪ���������ƣ�������Ҫ��������Ϊ�������ɵ�nc�ļ�������
		String elementU = pro.split(",")[1];//grib2�ļ�Ҫ������
		String elementV = pro.split(",")[2];
		String fileNameHead = pro.split(",")[0];//�ļ���ͷ����
		String timeStep = pro.split(",")[3];   //�ļ���ʱ�䲽��
		String elementName = pro.split(",")[4];    //дnc�ļ���Ҫ������
		String outPutPath = properties.getProperty("outPutPath");//���Ŀ��·��
		String filePath = properties.getProperty("filePath");//Grib�ļ�·��
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String fileDay = sdf.format(date);
		int hour = date.getHours();
		String fileDate = null;
		if(hour>=18){      //�жϣ������ǰʱ��Ϊ18����ǰ�����ȡ08ʱ�ε�grib2�ļ��������ȡ20ʱ�ε�grib2�ļ�
			fileDate = fileDay+"2000"+timeStep;
		}else{
			fileDate = fileDay+"0800"+timeStep;
		}
		String fileName = filePath + fileDay + "/" + fileNameHead + fileDate + ".GRB2";   //�ļ�������ע�͵���ҵ�񻯺��ٴ�
		System.out.println("begin open grib2 file -->"+fileName);
//		String fileName = "Z_NWGD_C_BABJ_P_RFFC_SCMOC-EDA10_201608030800_24003.GRB2";
		File file = new File(fileName);
		if(!file.exists()){   //�ж�Դ�ļ��Ƿ����,���治���ڣ���ֱ�Ӹ�����һ��ʱ�����ɵ�nc�ļ�
			getFileDate(outPutPath, elementName, "wind", date, timeStep);
			return;
		}
		
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
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("��ʼʱ�䣺"+time1);
		long begin = System.currentTimeMillis(); 
		//дnc�ļ�
		int lat_min,lat_max,lon_min,lon_max;
		BigDecimal a = new BigDecimal(timeRange);
		BigDecimal b = new BigDecimal(latRange);
		BigDecimal c = new BigDecimal(lonRange);
		BigDecimal d = a.multiply(b).multiply(c);
		BigDecimal e = new BigDecimal(Integer.MAX_VALUE);
		int count =1;
		while(e.compareTo(d)<0){
			count++;
			a = a.divide(new BigDecimal(count),0,BigDecimal.ROUND_HALF_UP);
			d = a.multiply(b).multiply(c);
		}
		int realTime = a.intValue();
		int realTimeCount = timeRange / realTime;
		for(int t=0;t<realTimeCount;t++){
			int arrayIndex = 0;
			short[]wsd = new short[realTime * (latRange) * (lonRange)];
			short[]wdr = new short[realTime * (latRange) * (lonRange)];
			double[]aa = new double[realTime];
			String filename = outPutPath + elementName +"/" + elementName+ "_" + fileDate + "_" + t + ".nc";
//			String filename = t+"windTest.nc";
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
	        for(int i =t*realTime;i<realTime*(t+1);i++){
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
						float wind_u = DoubleLineInter2(i, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"u");  //ͨ����ֵ��ȡu������ֵ
						float wind_v = DoubleLineInter2(i, lat_min, lat_max, lon_min, lon_max, lat_1Km, lon_1Km, lat_5Km_min, lat_5Km_max, lon_5Km_min, lon_5Km_max,"v");  //ͨ����ֵ��ȡv������ֵ
						wsd[arrayIndex * latRange * lonRange + j * lonRange + k] = (short) wind_u;
						wdr[arrayIndex * latRange * lonRange + j * lonRange + k] = (short) wind_v;
					}
				}
				aa[arrayIndex] = (i+1) * 3;
				arrayIndex++;
//				System.out.println(i+1);
			}
	        Array windS  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},wsd);
			Array windD  = Array.factory(DataType.SHORT, new int[]{realTime,latng.length,lonng.length},wdr);
			Array tiemArray  = Array.factory(DataType.DOUBLE, new int[]{realTime,latng.length,lonng.length},aa);
			dataFile.write(dataU, windS);
//			System.out.println("u success");
			dataFile.write(dataV, windD);
//			System.out.println("v success");
			dataFile.write(timeV, tiemArray);
			dataFile.write(latV, Array.factory(latng));
			dataFile.write(lonV, Array.factory(lonng));
			System.out.println("write netcdf success");
			dataFile.close();
		}
		gds.close();
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		System.out.println("����ʱ�䣺"+time2);
		System.out.println("����ʱ�䣺"+(end-begin)+"ms");
		
	}
	
	public float ValidData(float data){   //��ˮ�����ݼ�ֵУ��
		return data>999 ? 0.0F : data;
	}
	
	public float ValidTempData(float data){   //�¶����ݼ�ֵУ��
		if(data==9999){//���¶�Ϊ0�����¶�<-50,>50��ʱ��Ҳ��ȡ��һ�ε��ļ�
			data = 0;        //��ʱ�ȶ�Ϊ0���Ժ�������̻��ȡ��һ��ʱ�ε��ļ������Ǻ���γ�ȸ���Ե�����ݶ���9999.0
		}else{
			data = data - 273.15f;
		}
		return data;
	}
	
	public float ValidRhData(float data){   //���ʪ�����ݼ�ֵУ��
		return data>100 || data<0 ? 0.0F : data;
	}
	
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
		short Data = new BigDecimal(P * 10 + CorTemp).setScale(0, BigDecimal.ROUND_HALF_UP).shortValue();
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
		
		Q11 = ValidRhData(source3[time][0][lat_min][lon_min]);
		Q21 = ValidRhData(source3[time][0][lat_min][lon_max]);
		Q12 = ValidRhData(source3[time][0][lat_max][lon_min]);
		Q22 = ValidRhData(source3[time][0][lat_max][lon_max]);
		
		float data1 = (lat_5Km_max - lat_1Km) / fx;
		float data2 = (lat_1Km - lat_5Km_min) / fx;
		float R1 = data1 * Q11 + data2 * Q21;
		float R2 = data1 * Q12 + data2 * Q22;
		float P = ((lon_5Km_max - lon_1Km) / (lon_5Km_max - lon_5Km_min))  * R1 + ((lon_1Km - lon_5Km_min) / (lon_5Km_max - lon_5Km_min)) * R2;
		byte Data = new BigDecimal(P).setScale(0, BigDecimal.ROUND_HALF_UP).byteValue();
		return Data;
	}
	
	public String readHistoryFile(String point,Date date){ //��ȡ��ʷ�ļ�,Ŀǰ�Ĳ�����һ�����һ���ļ����о���̫�ã���������ռ���Ҫ���ĵ㣬Ȼ����������ȡ�ļ�
		Calendar cal = Calendar.getInstance();
		
		cal.add(Calendar.DAY_OF_MONTH, -1);
		
		Date historyDate = cal.getTime();
		
		String fileName = "";
		String fliePath = "";
		return null;
	}
	
	public float TempDingZheng(float data){//��ȡ�¶ȶ����ļ�
		//ÿһ���㶼����һ������ֵ��������ǰ���ɵ�nc�ļ���ȡ����ֵ
		String varName = "temp";
		String fileName = "Temperature_height_above_ground_201608050800_24003_0.nc";
		NetcdfFile ncFile = null;
		
		return 0f;
	}
	
}
