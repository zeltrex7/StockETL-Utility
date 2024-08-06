def upload_sftp():
    import pysftp

    # Define the SFTP server details
    hostname = '127.0.0.1'
    port = 2222
    username = 'abhishek'
    password = 'password'
    # Define connection options to disable host key checking
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Disable host key checking
    # Establish an SFTP connection
    with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
        import os
        print("Connection successfully established ... ")
        for i in os.listdir("./stocks_data"):
            file_name = './stocks_data/'+str(i)
            target_file_name = '/sftp_stocks_data/'+str(i)
            sftp.put(file_name,target_file_name)
        print(f"File uploaded successfully ")

upload_sftp()