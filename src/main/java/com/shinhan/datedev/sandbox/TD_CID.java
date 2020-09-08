package com.impala.udf.teradataudf;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.impala.udf.teradataudf.CommonUtils;

public class TD_CID extends UDF
{
	public TD_CID(String juminno) {
		//this.evaluate(juminno);
		System.out.println("input val :" + juminno);
	  }

	public String evaluate(String juminno) {
		String target_str = this.tdTrim(juminno);
		
		if ((target_str.length() == 0 ) || 
				(Character.isWhitespace(target_str.charAt(0))) ) {
			return "";
		}else {
			CommonUtils cu = new CommonUtils();
			//cu.getSha256(plainText);
			
			char[] in_id = new char[cu.LEN_IN_CUSTID + 1];
			char[] out_id = new char[cu.LEN_OUT_CUSTID + 1];
			
			String njuminno = this.toUpperString(this.tdTrim(juminno));
			in_id = cu.fillZeroNum(in_id);
			out_id = cu.fillZeroNum(out_id);
			
			in_id = cu.sprintf(in_id, njuminno);
			
			System.out.println("TD_CID : in_id > "+in_id.length+":"+new String(in_id));
			System.out.println("TD_CID : njuminno > "+njuminno.length()+":"+njuminno);		
			
			long res = cu.shb_create_custid(0, in_id, in_id.length-1, out_id, out_id.length-1);
			
			System.out.println(res);
		}
		
		return null;
	  }
    
	public String tdTrim(String str) {
		return str.trim();
	}
	
	public String toUpperString(String str) {
		return str.toUpperCase();
	}
	
	public static void main(String agrs[]) {
		String ta = " a V a b ";
		
		String tn = "11112222";
		// Bw9yecJRPntg2
		
		String juminno = "A";
		TD_CID tt = new TD_CID(tn);
		tt.evaluate(tn);
		
		/*
		CommonUtils cu = new CommonUtils();
		
		String target_str = tt.tdTrim(juminno);
		
		
		//cu.getSha256(plainText);
		
		char[] in_id = new char[cu.LEN_IN_CUSTID + 1];
		char[] out_id = new char[cu.LEN_OUT_CUSTID + 1];
		
		String njuminno = tt.toUpperString(tt.tdTrim(juminno));
		in_id = cu.fillZeroNum(in_id);
		out_id = cu.fillZeroNum(out_id);
		
		in_id = cu.sprintf(in_id, njuminno);
		
		cu.snprintf(40, "%d-%s%s", 0, "sHb", "A");
		*/
		//System.out.println(cu.testSHA256(tn));
		//System.out.println(tt.tdTrim(tn));
		//System.out.println(tt.tdTrim(ta));
		//System.out.println(tt.toUpperString(tn));
		//System.out.println(tt.toUpperString(ta));
		
		
		/*
		CommonUtils cu = new CommonUtils();
		//cu.shb_create_custid(0, tn, tn.length()-1, tn, tn.length()-1);
		String rltStr = cu.getSha256(tn);
		
		System.out.println(rltStr);
		
		byte[] bystStr;
        
        bystStr = rltStr.getBytes();
        System.out.println(bystStr);
        
        int bytelen =  bystStr.length;
        if(bytelen > 13){
           rltStr = new String(bystStr, 0, 13);
        }
        System.out.println(bystStr);
        System.out.println(rltStr);
        */
	}
}




/*
package com.cloudera.impala;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
*/
/**
 * Udf that returns true if two double arguments  are approximately equal.
 * Usage: > create fuzzy_equals(double, double) returns boolean
 *          location '/user/cloudera/hive-udf-samples-1.0.jar'
 *          SYMBOL='com.cloudera.impala.FuzzyEqualsUdf';
 *        > select fuzzy_equals(1, 1.000001);
 */
/*
public class FuzzyEqualsUdf extends UDF {
  public FuzzyEqualsUdf() {
  }

  public BooleanWritable evaluate(DoubleWritable x, DoubleWritable y) {
    double EPSILON = 0.000001f;
    if (x == null || y == null) return null;
    return new BooleanWritable(Math.abs(x.get() - y.get()) < EPSILON);
  }
}
*/