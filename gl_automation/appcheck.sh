if [ -z $1  ]; then
      echo "You must indicate the URL to be analyzed in AppCheck"
else

       . ./.p4config
       user=$P4USER
       var="$(echo $3| openssl enc -aes-128-cbc -a -d -salt -pass pass:121)"
      
        export LD_LIBRARY_PATH=/build/toolchain/lin64/openssl-1.0.2k/lib:$LD_LIBRARY_PATH
      /build/mts/toolchain/lin64/curl-7.51.0-openssl-1.0.2/bin/curl -X POST -u $user:$var -k -H 'Group:6' -H "Url:$1" --silent /dev/null https://appcheck.eng.vmware.com/api/fetch/> jsonfile.json

	 export LD_LIBRARY_PATH=/build/toolchain/lin64/openssl-1.0.2k/lib:$LD_LIBRARY_PATH
      /build/mts/toolchain/lin64/curl-7.51.0-openssl-1.0.2/bin/curl -X POST -u $user:$var -k -H 'Group:6' -H "Url:$2"  --silent /dev/null https://appcheck.eng.vmware.com/api/fetch/> jsonfile1.json
fi

