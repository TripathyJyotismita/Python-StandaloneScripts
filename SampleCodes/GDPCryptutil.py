import base64
from Crypto.Cipher import AES
import sys
from xml.dom import minidom

import StringIO
import cElementTree

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
        # ???? do a check for the length of key and iv - throw an error

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
#    print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt(sys.argv[1])+"|"
     print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").encrypt(sys.argv[1])

