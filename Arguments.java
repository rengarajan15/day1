package day1;

public class Arguments {
	
	
	// below method is created by us which can be created with any name
	public static void compare(String a, String b, String c) 
	{
		System.out.println(a);
		System.out.println(b);
		System.out.println(c);
		// as per Home work we should write code here to comstringspare 
		if(a.equals(b)) {
			System.out.println("a and b are same");
			
		}
		else
		{
			System.out.println("a and b not same");
		}
		System.out.println(a);
		System.out.println(b);
		System.out.println(c);
		
	}

	public static void main(String[] args) {
		
		String str1="one";
		String str2="two";
		String str3="three";
		
		//the below is how argument is passed to the method which we created above
		
		compare(str1, str2, str3);
			
	}

}
