/*
 GETL - based package in Groovy, which automates the work of loading and transforming data. His name is an acronym for "Groovy ETL".

 GETL is a set of libraries of pre-built classes and objects that can be used to solve problems unpacking,
 transform and load data into programs written in Groovy, or Java, as well as from any software that supports
 the work with Java classes.
 
 Copyright (C) 2013-2015  Alexsey Konstantonov (ASCRUS)

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Lesser General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License and
 GNU Lesser General Public License along with this program.
 If not, see <http://www.gnu.org/licenses/>.
*/

package getl.files

import getl.utils.Path

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

public class FileCleaner {
	private Path filePath = new Path();
	private Path arcPath = new Path();
	private HashMap<String, LinkedList<String>> arcs = new HashMap<String, LinkedList<String>>();
	
	/**
	 * 
	 * @param filemask
	 * @param archivemask
	 * @param zip
	 * @param clear
	 * @param delFolder
	 * @param findPath
	 * @param sysVar
	 * @param filter - Closure(String path, Map<String, String> values)
	 * @throws Exception
	 */
	public void clean (String filemask, String archivemask, boolean zip, boolean clear, boolean delFolder, String findPath, boolean sysVar, Closure filter) throws Exception {
		if (filemask == null || filemask.trim().length() == 0)
			throw new Exception("Required files path");
		
		if (zip && (archivemask == null || archivemask.trim().length() == 0))
			throw new Exception("Required archives path");
		
		if (!zip) archivemask = "C:/temp/clearfiles.zip";
		
		filePath.compile(filemask, sysVar);
		arcPath.compile(archivemask, false);
		
		prepareListFile(filter, findPath, sysVar);
		process(zip, clear, delFolder, findPath);
	}
	
	private String[] getFiles(String path) {
		File dir = new File(path);
		File[] f = dir.listFiles();
		
		if (f == null)
			return new String[0];
		
		LinkedList<String> l = new LinkedList<String>(); 
		for (int i = 0; i < f.length; i++) {
			if (f[i].isDirectory()) {
				String[] n = getFiles(path + "/" + f[i].getName());
				for (int x = 0; x < n.length; x++)
					l.add(n[x]);
			}
			else 
				if (f[i].isFile())
					l.add(path +"/" + f[i].getName());
		}
		return l.toArray(new String[0]);
	}
	
	private LinkedList<String> getEmptyDirs(String path, String[] findPath) {
		final String[] pathl = findPath;
	
		def filterName = { File file, String name ->
			String filename = (file.getParent() + "/" + file.getName() + "/" + name).replace('\\', '/').toLowerCase();
			for (int i = 0; i < pathl.length; i++) {
				if (filename.indexOf(pathl[i]) == 0)
					return true;
			}
			return false;
		}
				
		File dir = new File(path);
		File[] f;
		if (pathl == null)
			f = dir.listFiles();
		else
			f = dir.listFiles(new Filter(filterName));
			
		LinkedList<String> l = new LinkedList<String>();
		if (f == null) return l;
		for (int i = 0; i < f.length; i++) {
			if (f[i].isDirectory()) {
				LinkedList<String> n = getEmptyDirs(path + "/" + f[i].getName(), findPath);
				l.addAll(n);
			}
		}
		if (f.length == 0) l.add(path);
		return l;
	}
	
	private void prepareListFile (Closure filter, String findPath, boolean sysVar) {
		String[] r = getFiles(filePath.rootPath);
		Pattern pf = Pattern.compile(filePath.maskPath.toLowerCase());
		
		arcs.clear();
		//delFolders.clear();
		
		String[] paths = null;
		if (findPath != null && findPath.trim().length() > 0) {
			paths = findPath.split(";");
			for (int i = 0; i < paths.length; i++) 
				if (paths[i].trim().length() > 0) 
					paths[i] = paths[i].trim().toLowerCase();
				else
					paths[i] = null;
		}

		// �������� �� ������ ������
		for (int i = 0; i < r.length; i++) {
			String s = r[i];
			String sl = s.toLowerCase();
			
			String arc = arcPath.maskPath;
			HashMap<String, String> values = new HashMap<String, String>();
			
			// ��������� ���� �� ��������� � ����
			if (paths != null) {
				boolean inPath = (paths.length == 0);
				if (paths.length > 0)
					for (int p = 0; p < paths.length; p++)
						if (paths[p] != null && sl.indexOf(paths[p]) == 0) {
							inPath = true;
							break;
						}
				
				if (!inPath) continue;
			}

			// �������� ��������� � ���� �����
			Matcher mf = pf.matcher(sl);
			boolean added = mf.matches();
			if (added) {
				for (int x = 0; x < filePath.vars.length - ((sysVar)?2:0); x++) {
					String v = mf.group(x + 1);
					values.put(filePath.vars[x], v);
				}
			}
			
			// ������������� ��������� ����������
			if (added && sysVar) {
				File f = new File(s);
				
				Date fd = new Date(f.lastModified());
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				values.put("#file_date", df.format(fd));
				
				values.put("#file_size", String.valueOf(f.length()));
			}
			
			// �������� ������
			if (added && filter != null) {
				added = filter(s, values);
			}
			
			if (added) {
				// ��������� ��� ������
				values.each { key, value ->
					arc = arc.replace("{" + key + "}", value);
				}
				
				// ������� ���� � ������ ������ ��� ������
				LinkedList<String> fl = arcs.get(arc);
				if (fl == null) {
					fl = new LinkedList<String>();
					arcs.put(arc, fl);
				}
			
				fl.add(s);
			}
		}
	}
	
	// ��������� ��� ����� �� ����������
	private String arcName(String arcname) {
		File dir = new File(arcname.substring(0, arcname.lastIndexOf("/")));
		if (!dir.exists()) dir.mkdirs();
		
		File file = new File(arcname);
		if (!file.exists()) return arcname;
		
		String name = file.getName();
		int i = name.lastIndexOf('.');
		String ext = null;
		if (i >= 0) {
			ext = name.substring(i + 1);
			name = name.substring(0, i);
		}
		final String mask = (name + ".([0-9]{3})" + ((ext != null)?"." + ext:"")).replace(".", "[.]").toLowerCase();
		
		def filterName = { File filedir, String filename ->
			return (filename.toLowerCase().matches(mask));
		}
		
		String[] files = dir.list(new Filter(filterName));
		int num = 0;
		for (i = 0; i < files.length; i++) {
			String v = (ext != null)?files[i].substring(0, files[i].length() - ext.length() - 1):files[i];
			int p = v.lastIndexOf('.');
			v = v.substring(p + 1);
			int n = Integer.parseInt(v);
			if (n > num) num = n;
		}
		num++;
		
		String numStr = "00" + num;
		numStr = numStr.substring(numStr.length() - 3);
		arcname = file.getParent().replace('\\', '/') + "/" + name + "." + numStr + ((ext != null)?"." + ext:"");
		return arcname;
	}
	
	private void process (boolean zip, boolean clear, boolean delFolder, String findPath) throws IOException {
		arcs.each { key, value ->
			String a = key;
			LinkedList<String> f = value;
			
			if (zip) {
				a = arcName(a);
								
				byte[] buf = new byte[1024];
				ZipOutputStream out = new ZipOutputStream(new FileOutputStream(a));
				try {
					for (int fi = 0; fi < f.size(); fi++) {
						String fn = f.get(fi);
						FileInputStream ins = new FileInputStream(fn);
						try {
							File fname = new File(fn);
							
							String zp = fname.getParent();
							zp = zp.replace('\\', '/');
							if (zp.length() == filePath.rootPath.length())
								zp = "";
							else
								zp = zp.substring(filePath.rootPath.length() + 1) + "/";
							String zf = fname.getName();

							out.putNextEntry(new ZipEntry(zp + zf));
	
							int len;
							while ((len = ins.read(buf)) > 0) {
				            	out.write(buf, 0, len);
				            }
	
				            out.closeEntry();
						}
						finally {
							ins.close();
						}
			        }
				}
				finally {
					out.close();
				}
				
				if (clear) {
					for (int fi = 0; fi < f.size(); fi++) {
						File fn = new File(f.get(fi));
						fn.delete();
					}
				}
				
				System.out.println("Created archive \"" + a + "\", " + f.size() + " files " + ((clear)?"moved":"added"));
			}
			else
				if (clear) {
					HashMap<String, Integer> m = new HashMap<String, Integer>();
					for (int fi = 0; fi < f.size(); fi++) {
						File fn = new File(f.get(fi));
						String filepath = fn.getParent();
						Integer num = (Integer)m.get(filepath);
						if (num == null) num = 0;
						num++;
						m.put(filepath, num);
						fn.delete();
					}
					m.each { k, v ->
						System.out.println("Clear folder \"" + k + "\": " + v + " files deleted");
					}
				}
		}
		
		if (clear && delFolder) {
			String fPath[] = (findPath != null)?findPath.toLowerCase().split(";"):null;
			LinkedList<String> dl = getEmptyDirs(filePath.rootPath.toLowerCase(), fPath);
			String dm = (filePath.maskFolder!=null)?filePath.maskFolder.toLowerCase():null;
			for (int di = 0; di < dl.size(); di++) {
				String filename = dl.get(di);
				if (dm == null || filename.toLowerCase().matches(dm)) {
					File dn = new File(filename);
					dn.delete();
					System.out.println("Clear folder \"" + filePath.rootPath + "\": " + "deleted folder \"" + filename + "\"");
				}
			}
		}
	}
	
}
