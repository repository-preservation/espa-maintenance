Python 2.7.1+ (r271:86832, Apr 11 2011, 18:13:53) 
[GCC 4.5.2] on linux2
Type "copyright", "credits" or "license()" for more information.
>>> import os
>>> from os.path import join, getsize
>>> for root, dirs, files in os.walk('/tmp/tst'):
	print files

	
['one.txt', 'two.txt']
[]
[]
[]
>>> for root, dirs, files in os.walk('/tmp/tst'):
	print dirs
	print files

	
['sub']
['one.txt', 'two.txt']
['4.txt', '3.txt']
[]
[]
[]
[]
[]
>>> for root, dirs, files in os.walk('/tmp/tst')
SyntaxError: invalid syntax
>>> for root, dirs, files in os.walk('/tmp/tst'):
	print os.path.join(root, dirs, files)

	

Traceback (most recent call last):
  File "<pyshell#12>", line 2, in <module>
    print os.path.join(root, dirs, files)
  File "/usr/lib/python2.7/posixpath.py", line 66, in join
    if b.startswith('/'):
AttributeError: 'list' object has no attribute 'startswith'
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for f in files:
		print os.path.join(root, f)

		
/tmp/tst/one.txt
/tmp/tst/two.txt
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for d in dirs:
		for root2, dirs2, files2 in os.walk(os.path.join(root, d)):
			for f2 in files2:
				print os.path.join(root2, f2)

				
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for d in dirs:
		print d

		
sub
4.txt
3.txt
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for d in dirs:
		print d

		
sub
sub2
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for d in dirs:
		for r2, d2, f2 in os.walk(root, d):
			print f2

			
['one.txt', 'two.txt']
['4.txt', '3.txt']
['5.txt']
['4.txt', '3.txt']
['5.txt']
>>> for root, dirs, files in os.walk('/tmp/tst'):
	print files

	
['one.txt', 'two.txt']
['4.txt', '3.txt']
['5.txt']
>>> for root, dirs, files in os.walk('/tmp/tst'):
	print os.path.join(root, files)

	

Traceback (most recent call last):
  File "<pyshell#41>", line 2, in <module>
    print os.path.join(root, files)
  File "/usr/lib/python2.7/posixpath.py", line 66, in join
    if b.startswith('/'):
AttributeError: 'list' object has no attribute 'startswith'
>>> for root, dirs, files in os.walk('/tmp/tst'):
	for f in files:
		print os.path.join(root, f)

		
/tmp/tst/one.txt
/tmp/tst/two.txt
/tmp/tst/sub/4.txt
/tmp/tst/sub/3.txt
/tmp/tst/sub/sub2/5.txt
>>> 

for root, dirs, files in os.walk('/tmp/browse'):
for d in dirs:
for f in files:
print os.path.join(d, f)  
Adam Dosch/GEOG/C... so it's relative to your staging...  
Adam Dosch/GEOG/C... so you get output like:  
Adam Dosch/GEOG/C... 05/file
04/file
02/file
01/file
06/file
03/file
05/file1
05/file
 
Adam Dosch/GEOG/C... you're not using the same location to prep this as the remote location on 03, right?  
Adam Dosch/GEOG/C... local = os.path.join(root, f)
remote = re.sub(stagedir,remotedir, local)  
Adam Dosch/GEOG/C... a reg-ex seemed to be the easier than that stupid loop I sent you. easier to know the end location and to a reg-ex replacement  


#USE THIS
>>> stagedir = '/tmp/tst'
>>> remotedir = '/tmp/outbound'
>>> for root, dirs, files in os.walk(stagedir):
	for f in files:
		local = os.path.join(root, f)
		remote = re.sub(stagedir, remotedir, local)
		print remote

