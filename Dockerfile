FROM google/cloud-sdk
 COPY . ./
 RUN pip install --upgrade pip
 RUN pip install --no-cache-dir -r requirements.txt
 ENTRYPOINT [ "python3" ]
 CMD ["main.py"]
 EXPOSE 8080:8080
