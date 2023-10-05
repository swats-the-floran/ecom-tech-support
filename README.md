Parsers and an abstraction layer for many marketplace integrations. Scrtipts get the main data from elasticsearch logs or gtp xml feeds and enrich it with the data from Postgresql database (with ssh tunneling).

DOESN'T WORK WITH PYTHON 3.8 AND LOWER
no information about python 3.9

1. clone the repo:  
$ git clone https://git.puls.ru/a.popov/ecom-tech-support/-/tree/master/

2. change directory  
$ cd ecom-tech-support

3. create virtual environment  
$ python -m venv venv

4. enter virtual environment  
$ source venv/bin/activate

5. install dependencies  
$ pip install -r requirements.txt

6. change mode of scripts  
$ chmod ug+x *py

7. enter your ssh login/password in .env file

now you can use scripts like it is shown in documentation  
https://confluence.puls.ru/pages/viewpage.action?pageId=40738741

arguments list and description getting by -h option
![image](https://github.com/swats-the-floran/ecom-tech-support/assets/38055017/6d5b4b3d-50a9-4613-ad08-2b4b0b095395)

example of input and output of a script
![image](https://github.com/swats-the-floran/ecom-tech-support/assets/38055017/72126284-4c5d-44dd-9209-e02760013b03)

result of script usage
![image](https://github.com/swats-the-floran/ecom-tech-support/assets/38055017/ee8ffd21-c595-4754-be5c-9c704ed7fead)
