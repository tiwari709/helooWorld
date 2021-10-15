package com.simplilearn;

import java.util.Scanner;

public class MyCalulator {

	public static void main(String[] args) {
	
	while(true) {
	
		int x,y;
		
		Scanner sc =new Scanner(System.in);
		System.out.println("Enter the value of x");
	
		x=sc.nextInt();
		
		System.out.println("Enter the value of y");
		
		
		y=sc.nextInt();
		
		System.out.println("Enter the arthemetic symbol you want to operate on the numbers");
		String symbol=sc.next();
		switch(symbol)
		{
		
		case "+":
		System.out.println("Addition of two number is   " +(x+y));
		break;
		
		case "-":
			System.out.println("subtraction of two number is  " +(x-y));
			break;	
		case "*":

			System.out.println("Multiplication of the number is  "+(x*y));
			break;
		case "/":
			  
			System.out.println("qutionet when  " +x+ " is divided by   " +y+ " is " +(x/y));
			break;
		case "%":
			
			System.out.println("Remainder when  " +x+ "  is divided by   "+y+ " is "+(x%y));
			break;
			
		default :
				System.out.println("Enter a valid symbol");
				
		}
				
				System.out.println("Enter Y if you want to continue else press N");
				
				char choice;
				choice =sc.next().charAt(0);

				if (choice=='Y') {
					
					
				}
				
				else {
					break;
					}
				
		
	}	
		
	}

}
