package com.impala.udf.teradataudf;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;

public final class CommonUtils {
	final static int LEN_SHA256_DIGEST = 32;
	final static int LEN_DEST64 = 100;
	final static int LEN_IN_CUSTID = 30;
	final static int LEN_OUT_CUSTID = 13;
	final String NOSE_1_STR = "sHb";
	
	/*
	 
	 new Array(0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5, 0x3956C25B, 0x59F111F1,
                0x923F82A4, 0xAB1C5ED5, 0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
                0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174, 0xE49B69C1, 0xEFBE4786,
                0xFC19DC6, 0x240CA1CC, 0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
                0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7, 0xC6E00BF3, 0xD5A79147,
                0x6CA6351, 0x14292967, 0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
                0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85, 0xA2BFE8A1, 0xA81A664B,
                0xC24B8B70, 0xC76C51A3, 0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
                0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5, 0x391C0CB3, 0x4ED8AA4A,
                0x5B9CCA4F, 0x682E6FF3, 0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
                0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2)
	 * */
	
	public long shb_create_custid(long flag, char[] in_id, 
			int in_len, char[] out_id, long out_len) {
		
		long rc = 0;
		long ix = 0;
		long in_sil_len = 0;
		
		char[] sSrc = new char[LEN_IN_CUSTID+10+1];
		char[] sDest = new char[LEN_SHA256_DIGEST + 1];
		char[] Dset64 = new char[LEN_DEST64 + 1];
		char[] noise_str = new char[LEN_IN_CUSTID+10+1];
		
		sSrc = this.fillZeroNum(sSrc);
		sDest = this.fillZeroNum(sDest);
		//Dset64 = this.fillZeroNum(sSrc);
		noise_str = this.fillZeroNum(noise_str);
		
		System.out.println("CU flag > " + flag);
		
		
		
		if (in_len > LEN_IN_CUSTID)
			return -1;
		if (out_len != LEN_OUT_CUSTID)
			return -1;
		
		in_sil_len = in_id.length;
		
		String tmp_str = null;
		
		if (flag != 0) {
			if (in_sil_len == 1)
				tmp_str = this.snprintf(sSrc.length-1, "%d-%s%s", flag, NOSE_1_STR, in_id);
			else if (in_sil_len == LEN_IN_CUSTID)
				tmp_str = this.snprintf(sSrc.length-1, "%d-%s", flag, in_id);
			else
				tmp_str = this.snprintf(sSrc.length-1, "%d-%s", flag, in_id);
		}else {
			if (in_sil_len ==1)
				tmp_str = this.snprintf(sSrc.length-1, "%s%s", NOSE_1_STR, in_id);
			
			System.out.println(new String(sSrc));
			System.out.println(in_sil_len +":"+ in_id.length +":"+ sSrc.length);
			
	        System.arraycopy(in_id, 0, sSrc, 0,(in_id.length - 1));
	        
	        System.out.println(in_id +":"+ in_id.length);
	        System.out.println(sSrc+":"+sSrc.length);
	        
		}
		
		if (tmp_str != null) {
			sSrc = this.stringToCharArr(sSrc.length, tmp_str);
		}
		
		//noise_str = this.fillZeroNum(noise_str);
		
		System.out.println(">"+in_id.length+":"+new String(in_id));
		System.out.println(">>"+sSrc.length+":"+new String(sSrc));
		
		in_sil_len = sSrc.length;
		System.out.println(in_sil_len);
		for(int ixx = 0; ixx < in_sil_len; ixx++) {
			//System.out.println(ixx +":"+ noise_str[ixx] +":"+
			//			(int) (in_sil_len - 1 - ixx)+":"+sSrc[(int) (in_sil_len - 1 - ixx)]);
			noise_str[ixx] = (char)sSrc[(int) (in_sil_len - 1 - ixx)];
		}
		
		System.out.println(noise_str.length+":"+noise_str+":"+new String(noise_str));
		
		//String sha256Val = this.testSHA256(new String(sDest));
		System.out.println(new String(in_id)+":"+new String(out_id)+":"+new String(sSrc)+":"+
					new String(sDest)+":"+new String(noise_str));
		
		System.out.println(this.getSha256(new String(out_id)));
		System.out.println(this.getSha256(new String(in_id)));
		System.out.println(this.getSha256(new String(sSrc)));
		System.out.println(this.getSha256(new String(sDest)));
		System.out.println(this.getSha256(new String(noise_str)));
		
		this.testB64Sha256(new String(out_id));
		this.testB64Sha256(new String(in_id));
		this.testB64Sha256(new String(sSrc));
		this.testB64Sha256(new String(sDest));
		this.testB64Sha256(new String(noise_str));
		
		return 0;
	}
	
	public char[] fillZeroNum(char[] narr) {
		for(int i = 0; i < narr.length; i++)
			narr[i] = 0x00;
		return narr;
	}
	
	public static char[] sprintf(char[] carr, String tstr) {
		char[] sarr = tstr.toCharArray();
		
		for (int i=0; i < sarr.length; i++) {
			carr[i] = sarr[i];
		}
		
		return carr;
	}
	
	public char[] stringToCharArr(int charSize, String targetStr) {
		char[] charArr = new char[charSize];
		charArr = this.fillZeroNum(charArr);
		
		char[] tArr = targetStr.toCharArray();
		
		for(int i=0; i < tArr.length; i++) {
			charArr[i] = tArr[i];
		}
		
		return charArr; 
	}
	
	public static String snprintf( int size, String format, Object ... args ) {
        StringWriter writer = new StringWriter( size );
        PrintWriter out = new PrintWriter( writer );
        out.printf( format, args );
        out.close();
        
        System.out.println("snprintf >>> "+writer.toString());
        
        return writer.toString();
    }
	/*
	 5+zrvFkLyIs3Yfps0D10n4dGPau2cCGlxnaMJexos/I=
cPuVpyu84FG6wapm2CQ4Q2sIvLcgztwVeZss/6YOHd8=
SjuFJJl9FeXwU0Kxh8H5Di1LSGtxYAPacwpfg7XuYPQ=
f5yeMayCVsovJYWD3yYtvH1vaPKgMEPVyZpK5ac5bOk=
	 * */
	
	public static String getSha256(String plainText) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = plainText.getBytes(Charset.forName("UTF-8"));
            //byte[] bytes = plainText.getBytes(Charset.forName("CP949"));
            md.update(bytes);
            return Base64.getEncoder().encodeToString(md.digest());
        } catch (Exception e) {
            System.out.println("Sha256 error.");
            e.printStackTrace();
            return null;
        }
    }
	
	public static void testB64Sha256(String str){
		//String str = "11112222";
		//String str = String.format("%41", args)
		
		String SHA = "";
		
		try{

			MessageDigest sh = MessageDigest.getInstance("SHA-256"); 

			sh.update(str.getBytes()); 

			byte byteData[] = sh.digest();

			StringBuffer sb = new StringBuffer(); 

			for(int i = 0 ; i < byteData.length ; i++){
				sb.append(Integer.toString((byteData[i]&0xff) + 0x100, 16).substring(1));
			}

			SHA = sb.toString();
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace(); 
			SHA = null; 
		}
		
		System.out.println(SHA);
		
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			byte[] bytes = SHA.getBytes(Charset.forName("UTF-8"));
	        //byte[] bytes = plainText.getBytes(Charset.forName("CP949"));
	        md.update(bytes);
	        String ttt = Base64.getEncoder().encodeToString(md.digest());
	        
	        System.out.println("testB64Sha256 > "+ttt);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	public String testSHA256(String str){

		String SHA = ""; 

		try{

			MessageDigest sh = MessageDigest.getInstance("SHA-256"); 

			sh.update(str.getBytes()); 

			byte byteData[] = sh.digest();

			StringBuffer sb = new StringBuffer(); 

			for(int i = 0 ; i < byteData.length ; i++){
				sb.append(Integer.toString((byteData[i]&0xff) + 0x100, 16).substring(1));
			}

			SHA = sb.toString();
		}catch(NoSuchAlgorithmException e){
			e.printStackTrace(); 
			SHA = null; 
		}

		return SHA;
	}
	
}
