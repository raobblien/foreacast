package weather.nmc.pop.fc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import ucar.nc2.write.Nc4Chunking;
import ucar.nc2.write.Nc4ChunkingStrategy;

public class WeatherCode {

	/**
	 * @author Robin  ��������Ҫ������NC�ļ�
	 * 
	 */
	
	String elementName = "weatherCode";
	
	public static void main(String[] args) {
		WeatherCode weatherCode = new WeatherCode();
		weatherCode.ReadAndWriteNcFile();
	}
	
	public void ReadAndWriteNcFile(){   //��������Ҫ��Ҫ���������Ľ�ˮ�����¶ȵ�nc�ļ����ж�
		Date date = new Date();
		DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String time1=format.format(date);
		System.out.println("��ʼʱ�䣺"+time1);
		long begin = System.currentTimeMillis();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		int hour = date.getHours();
		String nowDateS = sdf.format(date);
		InputStream inputStream = Grib2ParserNg.class.getClassLoader().getResourceAsStream("configs/pro.properties");
	 	Properties properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String pro = properties.getProperty(elementName);
		String popPath = pro.split(",")[0]; //��ˮ��nc�ļ�path
		String tempPath = pro.split(",")[1]; //�¶�nc�ļ�path
		String cloudPath = pro.split(",")[2]; //����nc�ļ�path
		String outPath = properties.getProperty("outPutPath");
		
		String fileHour = null;
		if(hour>5&hour<18){  //�����ǰСʱΪ5-16�㣬����Ϊ��ˮ�����¶ȵ�Ԥ���ļ�Ϊ08��������ΪԤ���ļ�Ϊ20
			fileHour = "08";
		}else{
			fileHour = "20";
		}
		
		NetcdfFile popNcFile = null;
		NetcdfFile tempNcFile = null;
		NetcdfFile cloudNcFile = null;
		short[][][] popSource;
		short[][][] tempSource;
		short[][][] cloudSource;
		
		try {
			for(int j=0;j<2;j++){
				String popFileName = popPath  + nowDateS + fileHour + "00_24003_" + j + ".nc";
				String tempFileName = tempPath + nowDateS + fileHour + "00_24003_" + j + ".nc";
				String cloudFileName = cloudPath + nowDateS + fileHour + "00_24003_" + j + ".nc";
				
				File popFile = new File(popFileName);
				File tempFile = new File(tempFileName);
				File cloudFile = new File(cloudFileName);
				
				if(!popFile.exists() || !tempFile.exists() || !cloudFile.exists()){   //����һ���ļ�������
					System.out.println("source file not exist!");
					continue;  
				}
				
				popNcFile = NetcdfFile.open(popFileName);
				tempNcFile = NetcdfFile.open(tempFileName);
				cloudNcFile = NetcdfFile.open(cloudFileName);
				System.out.println("pop file path-->" + popFileName);
				System.out.println("temp file path-->" + tempFileName);
				System.out.println("cloud file path-->" + cloudFileName);
				
				Variable popV = popNcFile.findVariable("pop");
				Variable tempV = tempNcFile.findVariable("temp");
				Variable cloudV = cloudNcFile.findVariable("cloud");
				
				Variable latV = popNcFile.findVariable("lat");
				Variable lonV = popNcFile.findVariable("lon");
				Variable timeV = popNcFile.findVariable("time");
				Array lonArray = lonV.read();
				Array latArray = latV.read();
				Array timeArray = timeV.read();
				String timeLength = String.valueOf(timeArray.getSize()-1);
				String lonLength = String.valueOf(lonArray.getSize()-1);
				String latLength = String.valueOf(latArray.getSize()-1);
				String section = "0:" + timeLength + ",0:" + latLength + ",0:" + lonLength;
//				System.out.println(section);
				Array popData = popV.read(section);
				Array tempData = tempV.read(section);
				Array cloudData = cloudV.read(section);
				double[] time = (double[]) timeArray.copyTo1DJavaArray();
				float[] lon = (float[]) lonArray.copyTo1DJavaArray();
				float[] lat = (float[]) latArray.copyTo1DJavaArray();
				
				int timeRange = time.length;
				int latRange = lat.length;
				int lonRange = lon.length;
				
				popSource = (short[][][]) popData.copyToNDJavaArray();
				tempSource = (short[][][]) tempData.copyToNDJavaArray();
				cloudSource = (short[][][]) cloudData.copyToNDJavaArray();
				popData = null;
				tempData = null;
				cloudData = null;
				popNcFile.close();
				tempNcFile.close();
				cloudNcFile.close();
				
				System.gc();
				
				String outPutPath = outPath + elementName + "/" + elementName + "_" + nowDateS + fileHour + "00_24003_" + j + ".nc";
				System.out.println("netcdf out put path-->" + outPutPath);
				NetcdfFileWriter dataFile = null;
			    Nc4Chunking chunker = Nc4ChunkingStrategy.factory(Nc4Chunking.Strategy.standard,5,false);//��������д���ļ������ٶ����
				NetcdfFileWriter.Version version = NetcdfFileWriter.Version.netcdf4;
			    dataFile = NetcdfFileWriter.createNew(version, outPutPath,chunker);
				
			    Dimension xDim = dataFile.addDimension(null, "lat", latRange);
		        Dimension yDim = dataFile.addDimension(null, "lon", lonRange);
		        Dimension zDim = dataFile.addDimension(null, "time", timeRange);
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
				
		        byte[] data = new byte[timeRange * latRange * lonRange];
//		        double[] timeData = new double[timeRange];
		        Variable dataV = dataFile.addVariable(null, elementName, DataType.BYTE,dims_Element);
		        Variable latCodeV = dataFile.addVariable(null, "lat", DataType.FLOAT,dims_Lat);
		        Variable lonCodeV = dataFile.addVariable(null, "lon", DataType.FLOAT,dims_Lon);
		        Variable timeCodeV = dataFile.addVariable(null, "time", DataType.DOUBLE,dims_Time);
		        lonCodeV.addAttribute(new Attribute("units", "degrees_east"));
		        latCodeV.addAttribute(new Attribute("units", "degrees_north"));
		        dataFile.create();
		        
		        int arrayIndex = 0;
				for(int t=0;t<timeRange;t++){
					for(int i=0;i<latRange;i++){
						for(int k=0;k<lonRange;k++){
//							float pop = new BigDecimal(popSource[t][i][k]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
							float p = Float.valueOf(String.valueOf(popSource[t][i][k]));
							float pop=(float) (Math.round(p*10)/100.0);
//							float pop = Math.round((Float.valueOf(String.valueOf(popSource[t][i][k]))) * 10) / 10;
							byte code = 0;
							if(pop>0){  //  �н�ˮ,���¶�
								short temp = (short) (tempSource[t][i][k] / 10);
								if(temp <= -2){  //ѩ
									if(pop <= 0.5){  //Сѩ
										code = 14;
									}else if(pop > 0.5 & pop <= 1.5){ //��ѩ
										code = 15;
									}else if(pop > 1.5 & pop <= 4){ //��ѩ
										code = 16;
									}else{ //��ѩ
										code = 17;
									}
								}else if(temp <= 2 & temp > -2){  //���ѩ
									code = 6;
								}else{//��
									if(pop<=3){  //С��
										code = 7;
									}else if(pop > 3 & pop <= 10){ //����
										code = 8;
									}else if(pop > 10 & pop <= 20){ //����
										code = 9;
									}else if(pop > 20 & pop <= 50){ //����
										code = 10;
									}else if(pop > 50 & pop <= 100){ //����
										code = 11;
									}else{  //�ش���
										code = 12;
									}
								}
							}else{  //�޽�ˮ��������
//								float cloud = new BigDecimal(cloudSource[t][i][k]).divide(new BigDecimal(10)).setScale(BigDecimal.ROUND_HALF_UP).floatValue();
//								float cloud = (Float.valueOf(String.valueOf(cloudSource[t][i][k]))) / 10;
								float c = Float.valueOf(String.valueOf(cloudSource[t][i][k]));
								float cloud=(float) (Math.round(c*10)/100.0);
								if(cloud <= 20){ //����
									code = 0;
								}else if(cloud > 20 & cloud <= 80){ //����
									code = 1;
								}else{ //����
									code = 2;
								}
							}
							data[arrayIndex * latRange * lonRange + i * lonRange + k] = code;
						}
					}
//					timeData[arrayIndex] = (t+1) * 3;
					arrayIndex++;
					System.out.println(t+1);
				}
				System.out.println("��ʼд�ļ�");
				Array dataArray  = Array.factory(DataType.BYTE, new int[]{timeRange,latRange,lonRange},data);
				dataFile.write(dataV, dataArray);
//				Array timeCodeArray = Array.factory(DataType.DOUBLE, new int[]{timeRange},timeData);
				dataFile.write(timeCodeV, Array.factory(time));
				dataFile.write(latCodeV, Array.factory(lat));
				dataFile.write(lonCodeV, Array.factory(lon));
				System.out.println("write netcdf success!!!");
				dataFile.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		Date date1=new Date();
		String time2=format.format(date1);
		System.out.println("����ʱ�䣺"+time2);
		System.out.println("����ʱ�䣺"+(end-begin)+"ms");
	}

}
