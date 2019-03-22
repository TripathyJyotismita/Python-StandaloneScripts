import sys, os
from Crypto.Cipher import AES
import base64, StringIO
class GDPCrypt(object):
    def __init__(self,key,iv):
        """
        as of now, AES with block cipher and 16bit IV is supported
        """
        self.key=""
        self.iv=""
        if(len(key)%16 == 0 and len(iv)%16 == 0):
            self.key=key
            self.iv=iv
        else:
            print "Error the key and iv values should be multiples of 16"
            sys.exit(0)
        
    def encrypt(self,plaintext):
        obj = AES.new(self.key, AES.MODE_CBC, self.iv)
        txtLen=len(plaintext)
        padLen = 0
        if(txtLen%16 != 0):
            padLen = (16*((txtLen/16)+1))-txtLen
            plainTxt=str(plaintext.ljust(padLen+txtLen, '\x07'))
        else:
            plainTxt=plaintext 
        cipherText = obj.encrypt(plainTxt)
        cipherText = base64.b64encode(cipherText) #encoded in base64 to remove non-ascii chars
        return cipherText
    
    def decrypt(self,ciphertext):
        obj = AES.new(self.key, AES.MODE_CBC, self.iv)
        b=base64.b64decode(ciphertext)
        plainText = obj.decrypt(b)
        return plainText.rstrip('\x07')
  
if __name__=="__main__":
    inputFilename= "password_files.txt"
    outputFilename= "password_encrypted_files.txt"
    
    myMode = "encrypt" # set to encrypt or decrypt only
    #myMode = "decrypt"

    if not os.path.exists(inputFilename):
        print ('file %s  is not present' %(inputFilename))
        sys.exit()
    '''
    if os.path.exists(outputFilename):
        print ('This file %s is already peresent, it will overwrite the file.(C)ontinue or (Q)uit?' %(outputFilename))
        response = input('>')
        if not response.lower().startswith('c'):
               sys.exit()
    '''
    

    if myMode == 'encrypt':
        print 'i am doing encryption'
        fileObj = open(inputFilename)
        fileContent = fileObj.readlines()
        for line in fileContent:
            print 'line:', line
            for item in fileContent:
                en_res=GDPCrypt("glowballonalltix", "globa2b|!2banaly").encrypt(item)
                wfileObj=open(outputFilename, "w")
                fileInput= wfileObj.write(en_res)
        print "encryption done"
  
    elif myMode == "decrypt":
        print 'i am doing decryption'
        with open(outputFilename) as fileObj:
            fileContent = fileObj.readlines()
        for line in fileContent:
            for item in fileContent:
                en_res=GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt(item)
                with open(inputFilename, "w") as wfileObj:
                    fileInput= wfileObj.write(en_res)
        print "decryption done"
    else:
        print 'Please check myMode. valid values are encrypt or decrypt'
