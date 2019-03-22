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
    
#    def encryptXmlString(self,xmlString,tagNames):
#        tag = ""
#        xmldoc=minidom.parseString(xmlString)
#        for tag in tagNames:
#            tagdoc = xmldoc.getElementsByTagName(tag)
#            if tagdoc!=None:
#                if len(tagdoc)!=0 and len(tagdoc[0].childNodes)!=0:
#                    val = tagdoc[0].childNodes[0].nodeValue
#                    if val!="":
#                        tagdoc[0].childNodes[0].nodeValue = self.encrypt(val)           
#        return xmldoc.toxml()

    def encryptXmlString(self,xmlString, tagNames,attribNames=[]):
        if not attribNames: # no attribute is to be encrypted
            for event, elem in cElementTree.iterparse(StringIO.StringIO(xmlString)):
                if elem.text and elem.tag in tagNames:
                    elem.text = self.encrypt(str(elem.text))
        else: # tag as well as attributes
            for event, elem in cElementTree.iterparse(StringIO.StringIO(xmlString)):
                if elem.text and elem.tag in tagNames:
                    elem.text = self.encrypt(str(elem.text))
                if elem.attrib:
                    for attr in elem.attrib.keys():
                        if attr in attribNames:
                            elem.attrib[attr] = self.encrypt(str(elem.attrib[attr]))
        
        a = cElementTree.tostring(elem)
        return str(a)

    def decryptXmlString(self, xmlString, tagNames,attribNames=[]):
        if not attribNames: # no attribute is to be encrypted
            for event, elem in cElementTree.iterparse(StringIO.StringIO(xmlString)):
                if elem.text and elem.tag in tagNames:
                    elem.text = self.decrypt(str(elem.text))
        else: # tag as well as attributes
            for event, elem in cElementTree.iterparse(StringIO.StringIO(xmlString)):
                if elem.text and elem.tag in tagNames:
                    elem.text = self.decrypt(str(elem.text))
                if elem.attrib:
                    for attr in elem.attrib.keys():
                        if attr in attribNames:
                            elem.attrib[attr] = self.decrypt(str(elem.attrib[attr]))
        a = cElementTree.tostring(elem)
        return str(a)

if __name__=="__main__":
 #   xml = '<xml><a>			<b> Analytics</b></a><c> India</c></xml>'
 #   obj = GDPCrypt("glowballonalltix", "globa2b|!2banaly")
 #   enc_xml = obj.encryptXmlString(xml,['a','b','c'])
 #   dec_xml = obj.decryptXmlString(enc_xml,['a','b','c'])
 #   print 'Encrypted String:',enc_xml
 #   print 'Decrypted String:',dec_xml
     print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").encrypt(sys.argv[1])+"|"
#     print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").encrypt("21992-02-16")+"|"
#    print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt("NCz19td0nYHNIe6mbbUdUw==")+"|"
#    print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt("LXG0W9RriV+Y71eBvZSGpg==")+"|"
#    print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt(sys.argv[1])+"|"
     print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt(sys.argv[1]) +"|" 
#     print "|"+GDPCrypt("glowballonalltix", "globa2b|!2banaly").decrypt(sys.argv[1])
