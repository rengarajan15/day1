package day1;

import org.openqa.selenium.chrome.ChromeDriver;

public class Test {

	public static void main(String[] args) {
    System.setProperty("webdriver.chrome.driver", "./drivers/chromedriver.exe");
    ChromeDriver driver= new ChromeDriver();
    driver.get("https://drive.google.com/drive/u/0/my-drive");
    driver.findElementByXPath("//input[@type='email']").sendKeys("test");
	}

}
;