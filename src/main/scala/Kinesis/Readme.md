AWS Kinesis Project:
https://www.youtube.com/watch?v=_t3k6oX2mfc&t=1187s

For this I have followed the above video. As we dont have Cloud9 I have created an EC2 instance, SSH'd it into local pc and ran lab1.py in that VM.
Create an EC2 and add the below role for it
{
"Effect": "Allow",
"Action": [
"kinesis:PutRecord"
],
"Resource": "*"
}
running lab1.py on EC2:
SSH into EC2: ssh -i "your-key.pem" ec2-user@<your-ec2-public-ip>
sudo yum install python3  
Transfer Python file: scp -i "your-key.pem" your_script.py ec2-user@<ec2-ip>:/home/ec2-user/

sudo yum install python3-pip -y
pip3 install boto3
Run: python3 your_script.py




























# kinesisZeroToHero

## Maven repo
https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-elasticsearch7
