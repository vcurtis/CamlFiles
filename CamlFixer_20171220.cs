// CamlFixer
// By Sultan Alsawaf (July 2017)
// Last change: 2017-12-20
//
// Fixes common errors in CAML files and inserts missing metadata as well.
//
// ---------------------------------- Usage ----------------------------------
// This program asks for four parameters: the source directory containing the
// year folders, the intermediate destination directory, the final destination
// directory, and the years to be run.
//
// -Source directory parameter (first parameter):
// The source directory containing all the year folders is the full path to the parent
// folder of multiple folders corresponding to individual years. This parent folder will
// have multiple folders in it with names such as "1916 - Metadata Done" and "1943". An
// example of an actual path to provide for this is
// "\\ldcfs04\HistoricalStatutes\WORK-IN-PROGRESS CAML\1900 - 1959" (without quotes).
// The "1900 - 1959" folder contains the year folders for 1900 through 1959. The program
// will automatically copy what is needed from here.
//
// -Intermediate destination directory parameter (second parameter):
// The intermediate destination directory is the full path to the folder where you want the program
// to put year folders and actually process them. The only things that will copied from each of the
// year folders in the source directory are the "CAML" and "Metadata" subdirectories. Each
// year folder has two folders inside of it named something like "1912 CAML" and "1912 Metadata".
// These two folders are all that will be copied over to each year folder in the destination
// folder. This is because the "CAML" and "Metadata" folders' contents are all this program
// processes. Extra files or folders located in the year folder will not be copied over.
// Additionally, if the destination folder is not empty and has files that are being copied over
// from the source folder, then the existing files in the destination folder will NOT be
// overwritten. An error message will be printed to the console when this occurs.
// Example destination path (without quotes): "C:\Users\alsawasu\Documents"
//
// -Final destination directory parameter (third parameter):
// The final destination directory is an optional parameter that just specifies a directory
// to copy the final, processed year folders to. This feature is included to take advantage
// of the built-in multithreaded copy routines when copying between two different drives
// (e.g. a local drive and a remote drive). The intended use for this is to quickly copy the
// final year folders to a different drive than the one used in the intermediate destination
// parameter. An example of an actual path to provide for this is
// "\\ldcfs04\HistoricalStatutes\WORK-IN-PROGRESS CAML\New Metadata CAML\08.22.2017 CAML" (without quotes).
//
// -Years parameter (fourth parameter):
// This is the years to run the program for. You can input a range of years (e.g. "1930-1935", without
// quotes; 1930 and 1935 ARE included), and/or an individual year. Years can be delimited by anything
// other than a dash ("-"), new line, and number. Examples of valid input (without quotes):
// "1940-1964, 1890; 1894-1896" or "1955 1943 1930-1931" or "1915"
// If a year is input for which a year folder in the source directory does not exist, then that year
// will be ignored and an error message will be printed to the console.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace CamlFixer
{
    public class Program
    {
        static void Main(string[] args)
        {
            string srcDir = null, localDestDir = null, finalDestDir = null;
            var years = new List<string>();

            Console.WriteLine("Version: 2017-12-20" + Environment.NewLine);

            try {
                GetUserParams(ref srcDir, ref localDestDir, ref finalDestDir, ref years);
            } catch (Exception) {
                ReportErrorsAndExit();
                return;
            }

            // Record program run time for benchmarking purposes.
            var watch = Stopwatch.StartNew();

            Console.WriteLine(Environment.NewLine +
                "Copying source folders to intermediate destination..." + Environment.NewLine);

            // Only copy the subdirectories for each year, and exclude the CAML files themselves
            // as they will be saved onto the intermediate destination one-by-one immediately after
            // they are processed. The CAML files are saved like this to eliminate redundant I/O
            // operations in order to improve performance.
            CopyYearFolders(srcDir, localDestDir, years, true, true);

            // Start consumer thread that writes processed CAML content in memory to files.
            var fileTask = new Task(() => WriteCamlFilesTaskCb());
            fileTask.Start();

            for (var i = 0; i < years.Count; i++) {
                string year = years[i];

                Console.WriteLine("Running for year " + year + "...");
                string yearDirName = GetDirNameContaining(srcDir, year);
                string srcYearDir = Path.Combine(srcDir, yearDirName);
                string destYearDir = Path.Combine(localDestDir, yearDirName);
                FixCamlFiles(srcYearDir, destYearDir, year);

                if (i == (years.Count - 1))
                    GlobalVar.CamlFileBc.CompleteAdding();
                else
                    Console.Write(Environment.NewLine);
            }

            // Wait until all processed CAML files are written to their destination.
            fileTask.Wait();

            // Multithreaded file copy to final destination. This yields a significant speed-up
            // when the source and destination folders are on different drives, which they
            // typically are (e.g. source folder is on a C:\ drive, destination folder is on
            // a network drive).
            if (!String.IsNullOrWhiteSpace(finalDestDir) && finalDestDir != localDestDir) {
                Console.WriteLine(Environment.NewLine +
                    "Copying processed folders to final destination...");
                CopyYearFolders(localDestDir, finalDestDir, years, false, false);
            }

            watch.Stop();
            TimeSpan ts = watch.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine(Environment.NewLine + "Elapsed time: " + elapsedTime);

            ReportErrorsAndExit();
        }

        private static void ReportErrorsAndExit()
        {
            Console.Write(Environment.NewLine + Environment.NewLine);

            if (GlobalVar.ErrorCount > 0) {
                Console.WriteLine("Completed with the following " + GlobalVar.ErrorCount + " error(s):");

                foreach (var msg in GlobalVar.ErrorMsgList)
                    Console.WriteLine(msg);

                Console.WriteLine(Environment.NewLine + "Please review the errors, then press any key to exit.");
            } else {
                Console.WriteLine("Completed with 0 errors." + Environment.NewLine + "Press any key to exit.");
            }

            Console.ReadKey();
        }

        private static void GetUserParams(ref string srcDir, ref string localDestDir,
            ref string finalDestDir, ref List<string> years)
        {
            string msg;

            msg = "Enter the source directory with the year folders." +
                Environment.NewLine + "> ";
            Console.Write(msg);
            srcDir = Console.ReadLine();
            if (String.IsNullOrWhiteSpace(srcDir) || !Directory.Exists(srcDir)) {
                WriteConsoleErrorMsg("ERROR: Source directory doesn't exist!");
                throw new Exception();
            }

            msg = Environment.NewLine + "Enter the intermediate destination directory." +
                Environment.NewLine + "This should be on a local drive (like C:\\)." +
                Environment.NewLine + "Duplicate destination files won't be overwritten." +
                Environment.NewLine + "> ";
            Console.Write(msg);
            localDestDir = Console.ReadLine();
            if (String.IsNullOrWhiteSpace(localDestDir) || !Directory.Exists(localDestDir)) {
                WriteConsoleErrorMsg("ERROR: Intermediate destination directory doesn't exist!");
                throw new Exception();
            }
            if (localDestDir == srcDir) {
                WriteConsoleErrorMsg("ERROR: Intermediate directory is the same as the source directory!");
                throw new Exception();
            }

            msg = Environment.NewLine + "[OPTIONAL] Enter the final destination directory." +
                Environment.NewLine + "This should be on a remote drive." +
                Environment.NewLine + "Leave blank to skip this." +
                Environment.NewLine + "> ";
            Console.Write(msg);
            finalDestDir = Console.ReadLine();
            if (!String.IsNullOrWhiteSpace(finalDestDir) && !Directory.Exists(finalDestDir)) {
                WriteConsoleErrorMsg("ERROR: Final destination directory doesn't exist!");
                throw new Exception();
            }
            if (finalDestDir == srcDir) {
                WriteConsoleErrorMsg("ERROR: Final directory is the same as the source directory!");
                throw new Exception();
            }

            msg = Environment.NewLine + "Enter the years to run." +
                Environment.NewLine + "> ";
            Console.Write(msg);
            ParseUserYears(ref years);

            // Filter out non-existent years so the years can be divided evenly among tasks later.
            // This is not a serious error, unless there are no years left over after this.
            for (var i = years.Count - 1; i >= 0; i--) {
                string year = years[i];
                if (String.IsNullOrEmpty(GetDirNameContaining(srcDir, year))) {
                    WriteConsoleErrorMsg("ERROR: Couldn't find '" + year + "' year folder!");
                    years.RemoveAt(i);
                }
            }
            if (years.Count == 0)
                throw new Exception();
        }

        private static void ParseUserYears(ref List<string> years)
        {
            string input = Console.ReadLine();
            string[] splitInput = Regex.Split(input, "[^0-9-]+");

            foreach (var yearInput in splitInput) {
                if (yearInput.Contains('-')) {
                    if (!Regex.IsMatch(yearInput, "^[0-9]{4}-[0-9]{4}$")) {
                        WriteConsoleErrorMsg("ERROR: Invalid year range '" + yearInput + "'!");
                        throw new Exception();
                    }

                    // Add a range of years (formatted as "YYYY-YYYY").
                    int startYear = Int32.Parse(yearInput.Substring(0, 4));
                    int endYear = Int32.Parse(yearInput.Substring(5, 4));
                    for (var i = startYear; i < (endYear + 1); i++)
                        years.Add(i.ToString());
                } else {
                    if (Regex.IsMatch(yearInput, "^[0-9]{4}$"))
                        years.Add(yearInput);
                }
            }
        }

        private static void WriteConsoleErrorMsg(string errorMsg)
        {
            // Keep track of the number of errors encountered.
            lock (GlobalVar.ErrorCountLock) {
                GlobalVar.ErrorCount++;
                GlobalVar.ErrorMsgList.Add(errorMsg);
            }

            Console.WriteLine(errorMsg);
        }

        private static string GetDirNameContaining(string rootDir, string subStr)
        {
            // The year folders can have names such as "1963 - Metadata Done" or "1962",
            // so the folder name for a certain year cannot be hardcoded. The CAML and
            // Metadata folders can have inconsistent names as well.
            var finalDirName = string.Empty;

            if (String.IsNullOrWhiteSpace(subStr))
                return finalDirName;

            foreach (var dir in Directory.GetDirectories(rootDir)) {
                string dirName = dir.Substring(dir.LastIndexOf('\\') + 1);
                if (dirName.IndexOf(subStr, StringComparison.OrdinalIgnoreCase) < 0)
                    continue;

                if (!String.IsNullOrEmpty(finalDirName)) {
                    WriteConsoleErrorMsg(String.Format("{0} {1} {2}",
                        "ERROR: More than one folder named",
                        "'" + subStr + "'!",
                        "Using '" + finalDirName + "'."));
                    break;
                }

                finalDirName = dirName;
            }

            return finalDirName;
        }

        private static void CopyYearFolders(string srcDir, string destDir,
            List<string> years, bool onlyCopySubdirs, bool excludeCaml)
        {
            int nrTasks = Environment.ProcessorCount + 1;
            if (years.Count < nrTasks)
                nrTasks = years.Count;

            int yearsPerTask = years.Count / nrTasks;
            int remainder = years.Count % nrTasks;
            var taskList = new List<Task>();
            for (int i = 0, startIdx = 0; i < nrTasks; i++) {
                List<string> yearsToRun;
                if (remainder > 0) {
                    yearsToRun = years.GetRange(startIdx, yearsPerTask + 1);
                    startIdx += yearsPerTask + 1;
                    remainder--;
                } else {
                    yearsToRun = years.GetRange(startIdx, yearsPerTask);
                    startIdx += yearsPerTask;
                }
                var task = new Task(() => CopyYearFoldersTaskCb(srcDir, destDir,
                    yearsToRun, onlyCopySubdirs, excludeCaml));
                task.Start();
                taskList.Add(task);
            }

            Task.WaitAll(taskList.ToArray());
        }

        private static void CopyYearFoldersTaskCb(string srcDir, string destDir,
            List<string> years, bool onlyCopySubdirs, bool excludeCaml)
        {
            foreach (var year in years) {
                string yearDirName = GetDirNameContaining(srcDir, year);
                string yearSrcDir = Path.Combine(srcDir, yearDirName);
                string yearDestDir = Path.Combine(destDir, yearDirName);
                CopyYearDirectory(yearSrcDir, yearDestDir, onlyCopySubdirs, excludeCaml);
            }
        }

        private static void CopyYearDirectory(string srcDir, string destDir,
            bool onlyCopySubdirs, bool excludeCaml)
        {
            // Only copying subdirectories is needed sometimes because this program only
            // uses the two subdirectories for a single year folder (the "CAML" and "Metadata"
            // directories). There are very large files (PDF scans) in the first folder, so
            // copying them over is unnecessary and slow. Only the subdirectories will be copied
            // when onlyCopySubdirs is true.
            // excludeCaml exists because the CAML files will be copied one-by-one after being
            // processed.
            var dir = new DirectoryInfo(srcDir);

            if (!dir.Exists)
                return;

            // If the destination directory doesn't exist, create it.
            if (!Directory.Exists(destDir))
                Directory.CreateDirectory(destDir);

            string destDirName = destDir.Substring(destDir.LastIndexOf('\\') + 1);
            bool skipCamlFiles = excludeCaml &&
                destDirName.IndexOf("CAML", StringComparison.OrdinalIgnoreCase) >= 0;

            // Get the files in the directory and copy them to the new location.
            if (!onlyCopySubdirs && !skipCamlFiles) {
                try {
                    foreach (var file in dir.GetFiles())
                        file.CopyTo(Path.Combine(destDir, file.Name));
                } catch (Exception) {
                    WriteConsoleErrorMsg("ERROR: '" + destDirName +
                        "' Destination files already exist! Will NOT overwrite them.");
                }
            }

            foreach (var subdir in dir.GetDirectories())
                CopyYearDirectory(subdir.FullName, Path.Combine(destDir, subdir.Name), false, excludeCaml);
        }

        private static void FixCamlFiles(string srcYearDir, string destYearDir, string year)
        {
            string camlDirName = GetDirNameContaining(srcYearDir, "CAML");
            string srcCamlDir = Path.Combine(srcYearDir, camlDirName);
            string destCamlDir = Path.Combine(destYearDir, camlDirName);
            string srcMetadataDir = Path.Combine(srcYearDir, GetDirNameContaining(srcYearDir, "Metadata"));
            var camlErr = new CamlErrors();
            var camlMeasData = new CamlMeasureData();

            if (!Directory.Exists(srcCamlDir) || !Directory.Exists(srcMetadataDir)) {
                WriteConsoleErrorMsg("ERROR: " + year + ": Couldn't find CAML dir or Metadata dir!");
                return;
            }

            camlErr.ResErrorPath = Path.Combine(destYearDir, year + " Resolution Error List.txt");
            camlErr.StatErrorPath = Path.Combine(destYearDir, year + " Statute Error List.txt");

            ParseMetadataXmls(srcMetadataDir, ref camlMeasData);

            // Multithreaded CAML file processing.
            // The CAML files that need to be processed for each year are
            // divided evenly among threads.
            // One fewer task than normal is used by default here because there is a
            // background consumer thread writing CAML files to storage while these
            // tasks are running.
            string[] camlFiles = Directory.GetFiles(srcCamlDir);
            int nrTasks = Environment.ProcessorCount;
            if (camlFiles.Length < nrTasks)
                nrTasks = camlFiles.Length;

            int filesPerTask = camlFiles.Length / nrTasks;
            int remainder = camlFiles.Length % nrTasks;
            var taskList = new List<Task>();
            for (int i = 0, startIdx = 0; i < nrTasks; i++) {
                string[] filesToRun;
                if (remainder > 0) {
                    filesToRun = camlFiles.Skip(startIdx).Take(filesPerTask + 1).ToArray();
                    startIdx += filesPerTask + 1;
                    remainder--;
                } else {
                    filesToRun = camlFiles.Skip(startIdx).Take(filesPerTask).ToArray();
                    startIdx += filesPerTask;
                }
                var task = new Task(() => FixCamlFilesTaskCb(year, filesToRun, destCamlDir,
                    ref camlErr, ref camlMeasData));
                task.Start();
                taskList.Add(task);
            }

            Task.WaitAll(taskList.ToArray());

            // Find and report missing CAML files.
            ReportAllMissingCamlFiles(ref camlErr, camlMeasData, year);
            WriteAllErrorLists(camlErr);
        }

        private static void FixCamlFilesTaskCb(string year, string[] camlFiles, string destCamlDir,
            ref CamlErrors camlErr, ref CamlMeasureData camlMeasData)
        {
            foreach (var srcFile in camlFiles) {
                string fileName = Path.GetFileName(srcFile);
                string destFile = Path.Combine(destCamlDir, fileName);
                bool isResolution = fileName.Contains("CHR");

                // Perform a strict check on the file name to make sure it's formatted correctly.
                if (!Regex.IsMatch(fileName, "^CH(P|R)" + year + "[0-9]{5}\\.caml$")) {
                    string errorMsg = String.Format("{0}: {1}",
                            DateTime.Now.ToLongTimeString(),
                            "Unexpected file '" + fileName + "' in CAML directory.");
                    WriteErrorMsg(isResolution, ref camlErr, errorMsg);

                    // Copy the file since it won't be copied below.
                    File.Copy(srcFile, destFile);
                    continue;
                }

                // Get the "row" of metadata to use for this CAML file.
                MeasureDataRow row = null;

                try {
                    // Each file name is formatted as "CH*#########.caml". Extract the last 4 digits
                    // after "CH*#####" to get the chapter number, and use the fifth digit after
                    // the "CH*" to get the session number.
                    int chapNum = Int32.Parse(fileName.Substring(8, 4));
                    int sessionNum = fileName[7] - '0';
                    row = GetMeasureDataRow(isResolution, ref camlMeasData, sessionNum, chapNum);
                } catch (Exception e) {
                    string errorMsg = String.Format("{0}: {1}: {2}",
                            DateTime.Now.ToLongTimeString(),
                            Path.GetFileNameWithoutExtension(fileName),
                            e.Message);
                    WriteErrorMsg(isResolution, ref camlErr, errorMsg);

                    // Copy the file since it won't be copied below.
                    File.Copy(srcFile, destFile);
                    continue;
                }

                string xmlStr = null;

                try {
                    xmlStr = File.ReadAllText(srcFile, Encoding.UTF8);

                    DoCommonPreXmlReplacement(ref xmlStr);
                    if (isResolution)
                        DoResolutionXmlReplacement(ref xmlStr);
                    else
                        DoStatuteXmlReplacement(ref xmlStr);

                    // Insert the metadata into the CAML file.
                    XDocument doc = XDocument.Parse(xmlStr);
                    DoMeasureDocReplacement(ref doc, row, isResolution);
                    xmlStr = doc.Declaration.ToString() + Environment.NewLine + doc.ToString();

                    // Make changes that must be done after XDocument parses the file.
                    DoPostMeasureDocReplacement(ref xmlStr);
                } catch (Exception e) {
                    string prefix = String.Format("{0}: {1}: ",
                            DateTime.Now.ToLongTimeString(),
                            Path.GetFileNameWithoutExtension(fileName));
                    string errorMsg = prefix + e.Message;
                    errorMsg = errorMsg.Replace(Environment.NewLine, Environment.NewLine + prefix);
                    WriteErrorMsg(isResolution, ref camlErr, errorMsg);
                }

                if (!String.IsNullOrEmpty(xmlStr)) {
                    // Queue up the fixed CAML file to be written to its destination asynchronously.
                    var fileData = new CamlFileData();
                    fileData.Content = xmlStr;
                    fileData.DestinationPath = destFile;
                    GlobalVar.CamlFileBc.Add(fileData);
                } else {
                    File.Copy(srcFile, destFile);
                }
            }
        }

        private static void WriteCamlFilesTaskCb()
        {
            foreach (var data in GlobalVar.CamlFileBc.GetConsumingEnumerable()) {
                try {
                    File.WriteAllText(data.DestinationPath, data.Content, Encoding.UTF8);
                } catch (Exception e) {
                    WriteConsoleErrorMsg("ERROR: Couldn't write CAML file!" +
                        Environment.NewLine + e.Message);
                }
            }
        }

        private static void WriteErrorMsg(bool isResolution, ref CamlErrors camlErr, string errorMsg)
        {
            if (isResolution) {
                lock (camlErr.ResLock)
                    camlErr.ResErrorMsgs += errorMsg + Environment.NewLine;
            } else {
                lock (camlErr.StatLock)
                    camlErr.StatErrorMsgs += errorMsg + Environment.NewLine;
            }
        }

        private static void ReportAllMissingCamlFiles(ref CamlErrors camlErr,
            CamlMeasureData camlMeasData, string year)
        {
            ReportMissingCamlFiles(ref camlErr.ResErrorMsgs, camlMeasData.ResData, year, true);
            ReportMissingCamlFiles(ref camlErr.StatErrorMsgs, camlMeasData.StatData, year, false);
        }

        private static void ReportMissingCamlFiles(ref string errors,
            MeasureData[] data, string year, bool isResolution)
        {
            string prefix = isResolution ? "CHR" : "CHP";

            for (var sessionNum = 0; sessionNum < data.Length; sessionNum++) {
                var sessionData = data[sessionNum];
                if (sessionData == null)
                    continue;

                for (var i = 0; i < sessionData.Chapters.Length; i++) {
                    if (sessionData.Chapters[i].IsUsed)
                        continue;

                    string chapNumStr = sessionData.Chapters[i].ChapterNum;
                    if (String.IsNullOrWhiteSpace(chapNumStr))
                        continue;

                    string camlName = prefix + year + sessionNum + chapNumStr.PadLeft(4, '0');
                    string errorMsg = String.Format("{0}: {1}",
                        DateTime.Now.ToLongTimeString(),
                        camlName + ": CAML file is missing or named incorrectly.");
                    errors += errorMsg + Environment.NewLine;
                }
            }
        }

        private static void WriteAllErrorLists(CamlErrors camlErr)
        {
            WriteErrorList("Statute", camlErr.StatErrorPath, camlErr.StatErrorMsgs);
            WriteErrorList("Resolution", camlErr.ResErrorPath, camlErr.ResErrorMsgs);
        }

        private static void WriteErrorList(string camlType,
            string errorPath, string errorMsgs)
        {
            // Add a "No errors found" message if needed.
            if (String.IsNullOrEmpty(errorMsgs))
                errorMsgs = "No errors found in " + camlType + "s for this year." + Environment.NewLine;

            // Write all of the error messages in one go for performance reasons.
            File.WriteAllText(errorPath, errorMsgs);
        }

        private static MeasureDataRow GetMeasureDataRow(bool isResolution,
            ref CamlMeasureData camlMeasData, int sessionNum, int chapNum)
        {
            MeasureDataRow row;

            if (isResolution)
                row = GetUnusedMeasureDataRow(ref camlMeasData.ResData[sessionNum], chapNum);
            else
                row = GetUnusedMeasureDataRow(ref camlMeasData.StatData[sessionNum], chapNum);

            return row;
        }

        private static MeasureDataRow GetUnusedMeasureDataRow(ref MeasureData data, int chapNum)
        {
            MeasureDataRow row = null;
            int count = 0;

            for (var i = 0; i < data.Chapters.Length; i++) {
                if (data.Chapters[i].IsUsed)
                    continue;

                string chapNumStr = data.Chapters[i].ChapterNum;
                if (String.IsNullOrWhiteSpace(chapNumStr))
                    continue;

                if (Int32.Parse(chapNumStr) == chapNum) {
                    // Mark the chapter as "used."
                    data.Chapters[i].IsUsed = true;
                    row = data.Chapters[i];
                    count++;
                }
            }

            if (count > 1)
                throw new Exception(String.Format("{0} {1} {2}",
                    (count - 1) + " duplicate",
                    count > 2 ? "entries" : "entry",
                    "present in Excel sheet."));

            if (row == null)
                throw new Exception("Metadata info missing from Excel sheet.");

            return row;
        }

        private static void ParseMetadataXmls(string dir, ref CamlMeasureData camlMeasData)
        {
            // The ordering in resData and statData is important. The xml file names for extra
            // sessions are significant and are used to determine the index of the array used to hold
            // the data.
            foreach (var file in Directory.GetFiles(dir)) {
                if (Path.GetExtension(file) != ".xml")
                    continue;

                // In "XS" (extra session) file names, the extra session number precedes the "XS",
                // so it looks like this: "3XS" (this would correspond to the 3rd extra session).
                string fileName = Path.GetFileName(file);
                int xsIndex = fileName.ToUpper().IndexOf("XS");
                int sessionNum = xsIndex >= 0 ? fileName[xsIndex - 1] - '0' : 0;

                if (fileName.ToUpper().Contains("RES"))
                    camlMeasData.ResData[sessionNum] = GetMeasureData(file);
                else
                    camlMeasData.StatData[sessionNum] = GetMeasureData(file);
            }
        }

        private static MeasureData GetMeasureData(string metadataXmlPath)
        {
            var serializer = new XmlSerializer(typeof(MeasureData));
            var reader = new StreamReader(metadataXmlPath);
            var measData = new MeasureData();

            measData = (MeasureData)serializer.Deserialize(reader);
            reader.Close();

            return measData;
        }

        private static void DoResolutionXmlReplacement(ref string xmlStr)
        {
            xmlStr = xmlStr.Replace("id_Resolution", "Resolution");
            xmlStr = xmlStr.Replace("id=\"Resolution\"", "id=\"resolution\"");
            xmlStr = Regex.Replace(xmlStr, "<caml:Resolution id=\"id_\\S*?\"", "<caml:Resolution id=\"resolution\">");
            xmlStr = xmlStr.Replace("<caml:Bill id=\"bill\">", "<caml:Resolution id=\"resolution\">");
            xmlStr = xmlStr.Replace("</caml:Bill>", "</caml:Resolution>");
            xmlStr = Regex.Replace(xmlStr, "<caml:Preamble>.*?</caml:Preamble>", string.Empty);
            xmlStr = Regex.Replace(xmlStr, "<caml:Num>.*?</caml:Num>", string.Empty);

            // <caml:BillSection>           <caml:Whereas id="id_[GUID]">
            // </caml:Content>        -->   <caml:Content>
            // <p>Whereas                   <p>Whereas (<-- must be searched for case insensitively)
            xmlStr = Regex.Replace(xmlStr, "<caml:BillSection>\\s*</?caml:Content>\\s*<p>\\s*.{0,10}(whe|eas)",
                delegate (Match match)
                {
                    string matchStr = match.ToString();
                    matchStr = matchStr.Replace("<caml:BillSection>", "<caml:Whereas id=\"id_" + Guid.NewGuid() + "\">");
                    matchStr = matchStr.Replace("</caml:Content>", "<caml:Content>");
                    return matchStr;
                }, RegexOptions.IgnoreCase);

            // <caml:BillSection>     -->   <caml:Resolved id="id_[GUID]">
            // </?caml:Content>             <caml:Content>
            xmlStr = Regex.Replace(xmlStr, "<caml:BillSection>\\s*</?caml:Content>",
                delegate (Match match)
                {
                    string matchStr = match.ToString();
                    matchStr = matchStr.Replace("<caml:BillSection>", "<caml:Resolved id=\"id_" + Guid.NewGuid() + "\">");
                    matchStr = matchStr.Replace("</caml:Content>", "<caml:Content>");
                    return matchStr;
                }, RegexOptions.IgnoreCase);

            // <caml:Whereas>         -->   <caml:Whereas id="id_[GUID]">
            xmlStr = Regex.Replace(xmlStr, "<caml:Whereas>",
                delegate (Match match)
                {
                    return "<caml:Whereas id=\"id_" + Guid.NewGuid() + "\">";
                });

            // <caml:Resolution id="resolution">   -->   <caml:Resolution id="resolution">
            // <caml:BillSection>                        <caml:Whereas id="id_[GUID]">
            xmlStr = Regex.Replace(xmlStr, "<caml:Resolution id=\"resolution\">\\s*<caml:BillSection>",
                delegate (Match match)
                {
                    string matchStr = match.ToString();
                    matchStr = matchStr.Replace("<caml:BillSection>", "<caml:Whereas id=\"id_" + Guid.NewGuid() + "\">");
                    return matchStr;
                });

            // </caml:Whereas>            </caml:Whereas>
            // <caml:BillSection>   -->   <caml:Resolved id="id_[GUID]">
            // ...                        ...
            // </caml:Resolved>           </caml:Resolved>
            //
            // If the pattern after the singleline .*? does not exist, then this regex will
            // run very, very slowly.
            if (xmlStr.Contains("</caml:Resolved>")) {
                xmlStr = Regex.Replace(xmlStr, @"</caml:Whereas>\s*<caml:BillSection>.*?</caml:Resolved>",
                    delegate (Match match)
                    {
                        string matchStr = match.ToString();
                        matchStr = matchStr.Replace("<caml:BillSection>", "<caml:Resolved id=\"id_" + Guid.NewGuid() + "\">");
                        return matchStr;
                    }, RegexOptions.Singleline);
            }

            // <caml:Resolved id="id_*">         <caml:Resolved id="id_[GUID]">
            // ...                         -->   ...
            // </caml:BillSection>               </caml:Resolved>
            //
            // If the pattern after the singleline .*? does not exist, then this regex will
            // run very, very slowly.
            if (xmlStr.Contains("</caml:BillSection>")) {
                xmlStr = Regex.Replace(xmlStr, @"<caml:Resolved.*?>.*?</caml:BillSection>",
                    delegate (Match match)
                    {
                        string matchStr = match.ToString();
                        // Don't trample a valid "caml:BillSection" tag.
                        if (!Regex.IsMatch(matchStr, "<caml:BillSection id=\"id_\\S*?\">"))
                            matchStr = matchStr.Replace("</caml:BillSection>", "</caml:Resolved>");
                        return matchStr;
                    }, RegexOptions.Singleline);
            }

            // <caml:Whereas id="id_*">          <caml:Whereas id="id_[GUID]">
            // ...                         -->   ...
            // </caml:BillSection>               </caml:Whereas>
            //
            // If the pattern after the singleline .*? does not exist, then this regex will
            // run very, very slowly.
            if (xmlStr.Contains("</caml:BillSection>")) {
                xmlStr = Regex.Replace(xmlStr, @"<caml:Whereas.*?>.*?</caml:BillSection>",
                    delegate (Match match)
                    {
                        string matchStr = match.ToString();
                        // Don't trample a valid "caml:BillSection" tag.
                        if (!Regex.IsMatch(matchStr, "<caml:BillSection id=\"id_\\S*?\">"))
                            matchStr = matchStr.Replace("</caml:BillSection>", "</caml:Whereas>");
                        return matchStr;
                    }, RegexOptions.Singleline);
            }

            // <p>Resolved   -->   <p><i>Resolved</i>
            xmlStr = Regex.Replace(xmlStr, @"<p>\s*.{0,10}Resolved",
                delegate (Match match)
                {
                    string matchStr = match.ToString();
                    if (!matchStr.Contains("<i>")) {
                        int startTagIdx = matchStr.LastIndexOf("R", StringComparison.OrdinalIgnoreCase);
                        matchStr = matchStr.Insert(startTagIdx, "<i>");
                        matchStr += "</i>";
                    }
                    return matchStr;
                }, RegexOptions.IgnoreCase);

            AddSpaceBeforeContentNewlines(ref xmlStr);
        }

        private static void AddSpaceBeforeContentNewlines(ref string xmlStr)
        {
            // Isolate each <caml:Content> block's content.
            for (int contentStart, contentEnd = 0; ;) {
                contentStart = xmlStr.IndexOf("<caml:Content>", contentEnd);
                if (contentStart < 0)
                    break;

                contentStart += "<caml:Content>".Length;
                contentEnd = xmlStr.IndexOf("</caml:Content>", contentStart);
                if (contentEnd < 0)
                    break;

                string contentStr = xmlStr.Substring(contentStart, contentEnd - contentStart);

                // Iterate through each <p> block's content within the <caml:Content> block.
                for (int pStart, pEnd = 0; ;) {
                    pStart = contentStr.IndexOf("<p>", pEnd);
                    if (pStart < 0)
                        break;

                    pStart += "<p>".Length;
                    pEnd = contentStr.IndexOf("</p>", pStart);
                    if (pEnd < 0)
                        break;

                    string pStr = contentStr.Substring(pStart, pEnd - pStart);

                    // Insert a space before each new line.
                    pStr = Regex.Replace(pStr, @"(?<=\S)" + Environment.NewLine, " " + Environment.NewLine);

                    contentStr = contentStr.Remove(pStart, pEnd - pStart);
                    contentStr = contentStr.Insert(pStart, pStr);
                }

                // Inject the updated content block.
                xmlStr = xmlStr.Remove(contentStart, contentEnd - contentStart);
                xmlStr = xmlStr.Insert(contentStart, contentStr);
            }
        }

        private static void DoStatuteXmlReplacement(ref string xmlStr)
        {
            xmlStr = xmlStr.Replace("SEO.", "SEC.");

            xmlStr = Regex.Replace(xmlStr, "<p>.*?to read:(?= )",
                delegate (Match match)
                {
                    return match.ToString() + "</p><p>";
                });

            // This text replacement is meant to refresh all of the GUIDs in use, as some statutes have
            // the same GUID used across multiple elements. A delegate is used here to ensure that each
            // text replacement receives a unique GUID.
            xmlStr = Regex.Replace(xmlStr, "id=\"id_\\S*?\"",
                delegate (Match match)
                {
                    return "id=\"id_" + Guid.NewGuid() + "\"";
                });

            // Regex.Replace() is used on search strings that don't include regular expressions
            // because Regex.Replace()'s delegate functionality is needed to ensure that each
            // text replacement receives a unique GUID.
            xmlStr = Regex.Replace(xmlStr, "<caml:BillSection>",
                delegate (Match match)
                {
                    return "<caml:BillSection id=\"id_" + Guid.NewGuid() + "\">";
                });

            xmlStr = Regex.Replace(xmlStr, "<caml:LawSection>",
                delegate (Match match)
                {
                    return "<caml:LawSection id=\"id_" + Guid.NewGuid() + "\">";
                });

            xmlStr = Regex.Replace(xmlStr, "<caml:LawSectionVersion>",
                delegate (Match match)
                {
                    return "<caml:LawSectionVersion id=\"id_" + Guid.NewGuid() + "\">";
                });

            AddMissingContentEndTags(ref xmlStr);
        }

        private static void AddMissingContentEndTags(ref string xmlStr)
        {
            // Ensure that each "<caml:Content>" tag has an end tag.
            for (int billSectionEnd, contentStart, contentEnd = 0; ;) {
                contentStart = xmlStr.IndexOf("<caml:Content>", contentEnd);
                if (contentStart < 0)
                    break;

                contentStart += "<caml:Content>".Length;
                billSectionEnd = xmlStr.IndexOf("</caml:BillSection>", contentStart);
                if (billSectionEnd < 0)
                    break;

                contentEnd = xmlStr.IndexOf("</caml:Content>", contentStart);
                if (contentEnd >= 0 && contentEnd < billSectionEnd)
                    continue;

                contentEnd = billSectionEnd;
                xmlStr = xmlStr.Insert(contentEnd, "</caml:Content>");
            }
        }

        private static void RemoveExtraLegislators(ref string xmlStr)
        {
            // Remove extra instances of the "caml:Legislator" group.
            int startIndex = xmlStr.IndexOf("<caml:Legislator");
            if (startIndex < 0)
                return;

            startIndex = xmlStr.IndexOf("<caml:Legislator", startIndex + "<caml:Legislator".Length);
            if (startIndex < 0)
                return;

            int authorIndex = xmlStr.IndexOf("</caml:Authors>", startIndex + "<caml:Legislator".Length);
            if (authorIndex < 0)
                return;

            // Use the caml:Authors end-tag index for the LastIndexOf search to speed it
            // up since it searches backwards. The caml:Authors end tag comes right after
            // the end of the caml:Legislators group.
            int endIndex = xmlStr.LastIndexOf("</caml:Legislator>", authorIndex);
            if (endIndex < 0)
                return;

            xmlStr = xmlStr.Remove(startIndex, endIndex + "</caml:Legislator>".Length - startIndex);
        }

        private static void DoCommonPreXmlReplacement(ref string xmlStr)
        {
            // Remove junk at the beginning of the file, before the xml declaration.
            int xmlDecIndex = xmlStr.IndexOf("<?xml version");
            if (xmlDecIndex < 0)
                throw new Exception("XML declaration is missing.");
            xmlStr = xmlStr.Remove(0, xmlDecIndex);

            // Remove all comments.
            if (xmlStr.Contains("<!"))
                xmlStr = Regex.Replace(xmlStr, "<!.*?>", string.Empty, RegexOptions.Singleline);

            // Remove leading and trailing whitespace from inside of xml tags.
            xmlStr = Regex.Replace(xmlStr, @"(?<=<).*?(?=>)",
                delegate (Match match)
                {
                    return match.ToString().Trim();
                });

            // Remove xml start tags with invalid names (i.e. remove stray "<" characters).
            xmlStr = Regex.Replace(xmlStr, @"<\s*[^a-zA-Z/?]", string.Empty);

            // Fix xml end tags missing a "<" at the beginning.
            xmlStr = Regex.Replace(xmlStr, "(?<!<)/[a-zA-Z:]+( id=\"id_\\S*?\")?>",
                delegate (Match match)
                {
                    return "<" + match.ToString();
                });

            // Fix xml end tags missing a ">" at the end.
            xmlStr = Regex.Replace(xmlStr, @"</[a-zA-Z:]+(?=\s)",
                delegate (Match match)
                {
                    return match.ToString() + ">";
                });

            RemoveExtraLegislators(ref xmlStr);

            // Delete non-ASCII characters, including their trailing white spaces­
            // (except for certain unicode characters that are used in XMetaL).
            xmlStr = Regex.Replace(xmlStr, @"[^\u0000-\u007F¡¿¢£¤¥€¶§©®™ªº«»‘’“”…–—µƒ°·×÷±¹²³¼½¾¦]+\s*", string.Empty);

            // Some "ActionDate" labels have incorrect casing (i.e. "Actiondate"), which creates xml
            // errors.
            xmlStr = Regex.Replace(xmlStr, "Actiondate", "ActionDate", RegexOptions.IgnoreCase);

            xmlStr = xmlStr.Replace("utf-8", "UTF-8");
            xmlStr = xmlStr.Replace("DatePassed", "ActionDate");
            xmlStr = xmlStr.Replace("PassedDate", "ActionDate");
            xmlStr = xmlStr.Replace("<caml:Subject>NOT AVAILABLE.</caml:Subject>", "<caml:Subject></caml:Subject>");
            xmlStr = xmlStr.Replace("Oalifornia", "California");
            xmlStr = xmlStr.Replace("<P>", "<p>");
            xmlStr = xmlStr.Replace("</P>", "</p>");
        }

        private static void DoPostMeasureDocReplacement(ref string xmlStr)
        {
            // This appears to be introduced by XDocument (a stray ">" becomes "&gt;").
            xmlStr = xmlStr.Replace("&gt;", string.Empty);
        }

        private static void DoMeasureDocReplacement(ref XDocument doc, MeasureDataRow row,
            bool isResolution)
        {
            XElement measureDoc, description, legislativeInfo, authorsLegislator, element;
            XNamespace ns = doc.Root.Name.Namespace;
            string tag, errors = string.Empty, measureType = string.Empty;

            measureDoc = doc.Element(ns + "MeasureDoc");
            if (measureDoc == null)
                throw new Exception("'caml:MeasureDoc' tag is missing.");

            description = GetChildXElement(measureDoc, ns + "Description");
            legislativeInfo = GetChildXElement(description, ns + "LegislativeInfo");
            authorsLegislator = GetChildXElement(description, ns + "Authors");

            try {
                authorsLegislator = GetChildXElement(authorsLegislator, ns + "Legislator");
            } catch (Exception) {
                // Some CAML files use caml:Committee instead of caml:Legislator.
                authorsLegislator = GetChildXElement(authorsLegislator, ns + "Committee");
            }

            // Measure type.
            tag = "MeasureType";
            try {
                element = GetChildXElement(legislativeInfo, ns + tag);
                measureType = GetRowElement(row.MeasureType, tag);
                element.SetValue(measureType);
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Measure num.
            tag = "MeasureNum";
            try {
                element = GetChildXElement(legislativeInfo, ns + tag);
                element.SetValue(GetRowElement(row.MeasureNum, tag));
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Measure state.
            tag = "MeasureState";
            try {
                element = GetChildXElement(legislativeInfo, ns + tag);
                element.SetValue("CHP");
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Chapter type.
            tag = "ChapterType";
            try {
                element = GetChildXElement(legislativeInfo, ns + tag);
                element.SetValue(isResolution ? "CHR" : "CHP");
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Chapter num.
            tag = "ChapterNum";
            try {
                element = GetChildXElement(legislativeInfo, ns + tag);
                element.SetValue(GetRowElement(row.ChapterNum, tag));
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Author name.
            tag = "Name";
            try {
                TextInfo ti = CultureInfo.CurrentCulture.TextInfo;
                element = GetChildXElement(authorsLegislator, ns + tag);
                element.SetValue(ApplySpecialCaps(ti.ToTitleCase(GetRowElement(row.Name, tag).ToLower())));
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Author text.
            tag = "AuthorText";
            try {
                element = GetChildXElement(description, ns + tag);
                element.SetValue(GetRowElement(row.AuthorText, tag));
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // Digest text.
            tag = "DigestText";
            try {
                element = GetChildXElement(description, ns + tag);
                element.SetValue("Please be advised that the statutes in this database " +
                            "for years prior to 1989 were scanned from a hard copy " +
                            "original, and therefore may contain inaccuracies.");
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            // House name.
            tag = "House";
            try {
                element = GetChildXElement(authorsLegislator, ns + tag);
                if (Char.ToUpper(measureType[0]) == 'A')
                    element.SetValue("ASSEMBLY");
                else
                    element.SetValue("SENATE");
            } catch (Exception e) {
                errors += e.Message + Environment.NewLine;
            }

            if (!String.IsNullOrEmpty(errors))
                throw new Exception(errors.TrimEnd());
        }

        private static XElement GetChildXElement(XElement parent, XName childName)
        {
            if (parent.Elements(childName).Count() > 1)
                throw new Exception("'caml:" + childName.LocalName + "'" +
                    " metadata tag is present more than once.");

            parent = parent.Element(childName);
            if (parent == null)
                throw new Exception("'caml:" + childName.LocalName + "'" +
                    " metadata tag is missing or not in the correct place.");

            return parent;
        }

        private static string GetRowElement(string element, string name)
        {
            if (String.IsNullOrWhiteSpace(element))
                throw new Exception("'" + name + "' field is empty in Excel sheet.");
            return element.Trim();
        }

        private static string ApplySpecialCaps(string name)
        {
            // Some names like "McCorquodale" and "O'Connell" have a second capitalized
            // letter in them, so those letters need to be capitalized.
            string[] specialNames = { "Mac", "Mc", "'" };

            foreach (var prefix in specialNames) {
                int index = 0;

                // Use a second loop here in case the same prefix is used in both the first
                // and last name (i.e. "McConnell McCorquodale").
                while (true) {
                    index = name.IndexOf(prefix, index, StringComparison.OrdinalIgnoreCase);
                    if (index < 0)
                        break;

                    index += prefix.Length;
                    if (index > name.Length - 1)
                        break;

                    var sb = new StringBuilder(name);
                    sb[index] = Char.ToUpper(sb[index]);
                    name = sb.ToString();
                }
            }

            return name;
        }

        public static class GlobalVar
        {
            public static List<string> ErrorMsgList = new List<string>();
            public static object ErrorCountLock = new object();
            public static int ErrorCount = 0;
            public static BlockingCollection<CamlFileData> CamlFileBc = new BlockingCollection<CamlFileData>();
        }

        public class CamlFileData
        {
            public string DestinationPath;
            public string Content;
        }

        public class CamlErrors
        {
            public object ResLock = new object();
            public string ResErrorMsgs = string.Empty;
            public string ResErrorPath;
            public object StatLock = new object();
            public string StatErrorMsgs = string.Empty;
            public string StatErrorPath;
        }

        public class CamlMeasureData
        {
            // Each MeasureData element represents data from one session.
            // There will never be more than 10 sessions in a single year.
            public MeasureData[] ResData = new MeasureData[10];
            public MeasureData[] StatData = new MeasureData[10];
        }

        [XmlRoot("Measure-data", IsNullable = false)]
        public class MeasureData
        {
            [XmlElement("row")]
            public MeasureDataRow[] Chapters;
        }

        public class MeasureDataRow
        {
            public string MeasureType;
            public string MeasureNum;
            public string ChapterNum;
            public string Name;
            public string AuthorText;
            public bool IsUsed = false;
        }
    }
}