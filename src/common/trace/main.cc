#include "analyzer.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using simm::trace::TraceAnalyzer;

std::vector<std::string> ScanTraceFiles() {
  std::vector<std::string> files;

  DIR *dir = opendir("/dev/shm");
  if (!dir) {
    return files;
  }

  struct dirent *entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (strstr(entry->d_name, ".simm_trace.") != nullptr) {
      std::string full_path = std::string("/dev/shm/") + entry->d_name;
      struct stat st;
      if (stat(full_path.c_str(), &st) == 0 && S_ISREG(st.st_mode)) {
        files.push_back(full_path);
      }
    }
  }

  closedir(dir);
  return files;
}

void PrintUsage(const char *prog_name) {
  std::cout << "Usage: " << prog_name << " [OPTIONS] [SHM_PATH]\n"
            << "\n"
            << "Options:\n"
            << "  -a, --all          Print all trace details\n"
            << "  -l, --list         List all available trace files\n"
            << "  -o, --output FILE  Export trace data to JSON file (for Python analysis)\n"
            << "  -h, --help         Show this help message\n"
            << "\n"
            << "Arguments:\n"
            << "  SHM_PATH           Path to shared memory file (default: scan /dev/shm/)\n"
            << "\n"
            << "Examples:\n"
            << "  " << prog_name << "                    # Analyze all trace files\n"
            << "  " << prog_name << " -a                 # Analyze and print all traces\n"
            << "  " << prog_name << " -l                 # List available trace files\n"
            << "  " << prog_name << " -o traces.json     # Export to JSON file\n"
            << "  " << prog_name << " /dev/shm/.simm_trace.helloworld_client.12345\n"
            << std::endl;
}

int main(int argc, char *argv[]) {
  bool print_all = false;
  bool list_only = false;
  std::string shm_path;
  std::string output_file;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-a" || arg == "--all") {
      print_all = true;
    } else if (arg == "-l" || arg == "--list") {
      list_only = true;
    } else if (arg == "-o" || arg == "--output") {
      if (i + 1 < argc) {
        output_file = argv[++i];
      } else {
        std::cerr << "Error: -o requires a filename" << std::endl;
        PrintUsage(argv[0]);
        return 1;
      }
    } else if (arg == "-h" || arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    } else if (arg[0] != '-') {
      shm_path = arg;
    } else {
      std::cerr << "Unknown option: " << arg << std::endl;
      PrintUsage(argv[0]);
      return 1;
    }
  }

  if (list_only) {
    std::vector<std::string> files = ScanTraceFiles();
    if (files.empty()) {
      std::cout << "No trace files found in /dev/shm/" << std::endl;
    } else {
      std::cout << "Found " << files.size() << " trace file(s):" << std::endl;
      for (const auto &file : files) {
        struct stat st;
        if (stat(file.c_str(), &st) == 0) {
          std::cout << "  " << file << " (" << st.st_size << " bytes)" << std::endl;
        }
      }
    }
    return 0;
  }

  TraceAnalyzer analyzer;
  bool success = false;

  if (!shm_path.empty()) {
    std::cout << "\n=== Analyzing: " << shm_path << " ===" << std::endl;
    if (analyzer.Analyze(shm_path)) {
      analyzer.PrintStatistics();
      if (print_all) {
        analyzer.PrintAllTraces();
      }
      if (!output_file.empty()) {
        analyzer.ExportToJson(output_file);
      }
      success = true;
    }
  } else {
    std::vector<std::string> files = ScanTraceFiles();
    if (files.empty()) {
      std::cerr << "No trace files found in /dev/shm/" << std::endl;
      std::cerr << "Use -l to list available files" << std::endl;
      return 1;
    }

    for (const auto &file : files) {
      std::cout << "\n=== Analyzing: " << file << " ===" << std::endl;
      if (analyzer.Analyze(file)) {
        analyzer.PrintStatistics();
        if (print_all) {
          analyzer.PrintAllTraces();
        }
        if (!output_file.empty()) {
          std::string file_output = output_file;
          size_t last_dot = file_output.find_last_of('.');
          if (last_dot != std::string::npos) {
            file_output = file_output.substr(0, last_dot) + "_" + file.substr(file.find_last_of('/') + 1) +
                          file_output.substr(last_dot);
          }
          analyzer.ExportToJson(file_output);
        }
        success = true;
      } else {
        std::cerr << "Failed to analyze: " << file << std::endl;
      }
    }
  }

  if (!success) {
    return 1;
  }

  return 0;
}
