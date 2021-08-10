#pip3.6 install selenium
#http://pythonstudy.xyz/python/article/404-%ED%8C%8C%EC%9D%B4%EC%8D%AC-Selenium-%EC%82%AC%EC%9A%A9%ED%95%98%EA%B8%B0

#Make sure it¡¯s in your PATH, e. g., place it in /usr/bin or /usr/local/bin.
#https://pypi.org/project/selenium/

#firefox webdrvier download
#https://github.com/mozilla/geckodriver/releases

#chrome install & download
#https://linuxize.com/post/how-to-install-google-chrome-web-browser-on-centos-7/

#centos7 crhome version check
## google-chrome --version

#headless chrome
#https://beomi.github.io/2017/09/28/HowToMakeWebCrawler-Headless-Chrome/

from selenium import webdriver

options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")

# or options.add_argument("--disable-gpu")
driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=options)

driver.get('http://chakkhan.com/crontab?')
#driver.get('https://naver.com/')
driver.implicitly_wait(3)
driver.get_screenshot_as_file('naver_main_headless.png')

#print( driver.page_source )
#print( driver.find_element_by_tag_name('body').text )

driver.quit()