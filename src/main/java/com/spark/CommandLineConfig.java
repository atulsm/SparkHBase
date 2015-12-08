package com.spark;

/**
 * 
 * Supports following properties
 * 		-sendToHbase -sendToES -parallel -duration=10 
 * 
 * 
 * @author satul
 *
 */
public class CommandLineConfig {	
	public int duration = 10;	
	public boolean sendToHbase = false;
	public boolean sendToES = false;
	public boolean parallelSave = false;
	
	public CommandLineConfig(String[] args){		
		if(args==null || args.length==0){
			return;
		}
		
		for(String arg: args){
			if(arg.equals("-sendToHbase")){
				sendToHbase=true;				
			}else if(arg.equals("-sendToES")){
				sendToES=true;				
			}else if(arg.equals("-parallel")){
				parallelSave=true;				
			}else if(arg.startsWith("-duration=")){
				int idx = arg.indexOf("-duration=");
				duration= Integer.parseInt(arg.substring(idx+10));							
			}else{
				System.out.println("Unsupported argument " + arg);
				System.exit(0);
			}			
		}	
	}
	
	@Override
	public String toString() {
		return new StringBuilder().append("duration=").append(duration).append(",sendToHbase=").append(sendToHbase)
				.append(",sendToES=").append(sendToES).append(",parallelSave=").append(parallelSave).toString();
	}
	
	public static void main(String[] args) {
		CommandLineConfig config = new CommandLineConfig(new String[]{"-sendToHbase","-sendToES","-parallel","-duration=5"});
		System.out.println(config);
		System.out.println(new CommandLineConfig(null));
	}

}
