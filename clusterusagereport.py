
import argparse
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime,timedelta

def occupancy(data_list,curr_date,runtime,proc_num):
    end_date = (datetime.fromisoformat(curr_date) + timedelta(minutes=runtime)).isoformat()
    next_day = (datetime.fromisoformat(curr_date[:10]) + timedelta(days=1)).isoformat()
    diff = datetime.fromisoformat(next_day) - datetime.fromisoformat(curr_date)
    hour,min,sec = str(diff).split(':')
    if hour.find('day') != 0:
        left_runtime = 24.0*60
    else:
        left_runtime = float(hour)*60 + float(min) + float(sec)/60
    if runtime > left_runtime:
        data_list.append([curr_date[:10],float(proc_num)*left_runtime/60])
        new_remain_runtime = runtime - left_runtime
        num_days = int(new_remain_runtime/60/24)
        if num_days != 0:
            for i in range(num_days):
                new_date = (datetime.fromisoformat(curr_date[:10]) + timedelta(days=i+1)).isoformat()[:10]
                data_list.append([new_date,float(proc_num)*24])
            final_time = new_remain_runtime - num_days*24*60
            if final_time > 0:
                data_list.append([end_date[:10],float(proc_num)*final_time/60])
        else:
            new_date = (datetime.fromisoformat(curr_date[:10]) + timedelta(days=1)).isoformat()[:10]
            data_list.append([new_date,float(proc_num)*new_remain_runtime/60])
    else:
        data_list.append([curr_date[:10],float(proc_num)*runtime/60])
    return data_list

def data_collection(begin, end):
    rd_cpu_info,rd_cpu_occupancy = [],[]
    rd_gpu_info,rd_gpu_occupancy = [],[]
    client_cpu_info,client_cpu_occupancy = [],[]
    client_gpu_info,client_gpu_occupancy = [],[]
    BEGIN_TIME = "-S " + begin
    END_TIME = "-E " + end
    FORMAT = "--format=User%-16,Account%-16,Submit,Start,AllocCPUS,AllocTRES%-40,CPUTimeRAW"
    info = subprocess.Popen(['sacct', BEGIN_TIME, END_TIME, FORMAT, '--parsable2', '--noheader'], stdout=subprocess.PIPE)
    for line in info.stdout:
        data_point = line.decode('utf8').strip().split('|')
        start_time = datetime.fromisoformat(begin)
        stop_time = datetime.fromisoformat(end)
        if data_point[0] != '' and data_point[-1] != '0':
            job_submit_time = datetime.fromisoformat(data_point[2])
            job_start_time = datetime.fromisoformat(data_point[3])
            if job_submit_time >= start_time and job_submit_time <= stop_time:
                date = data_point[2]
                account = data_point[1]
                queued_time = (job_start_time-job_submit_time).total_seconds()/60
                cpu_num = data_point[4]
                cpu_hour = float(data_point[6])/3600
                runtime = cpu_hour/float(cpu_num)*60
                if "gres" in data_point[5]:
                    gpu_num = data_point[5].split(',')[2][9:]
                    gpu_hour = float(gpu_num)*runtime/60
                    if account == 'tandemai':
                        rd_gpu_info.append([date,account,queued_time,runtime,gpu_num,gpu_hour])
                        rd_gpu_occupancy = occupancy(rd_gpu_occupancy,data_point[3],runtime,gpu_num)
                    else:
                        client_gpu_info.append([date,account,queued_time,runtime,gpu_num,gpu_hour])
                        client_gpu_occupancy = occupancy(client_gpu_occupancy,data_point[3],runtime,gpu_num)
                else:
                    if account == 'tandemai':
                        rd_cpu_info.append([date,account,queued_time,runtime,cpu_num,cpu_hour])
                        rd_cpu_occupancy = occupancy(rd_cpu_occupancy,data_point[3],runtime,cpu_num)
                    else:
                        client_cpu_info.append([date,account,queued_time,runtime,cpu_num,cpu_hour])
                        client_cpu_occupancy = occupancy(client_cpu_occupancy,data_point[3],runtime,cpu_num)
    return rd_cpu_info, rd_gpu_info, client_cpu_info, client_gpu_info, rd_cpu_occupancy, rd_gpu_occupancy, client_cpu_occupancy, client_gpu_occupancy

def percentile(data, metric):
    return data[metric].quantile(q=0.5), data[metric].quantile(q=0.75)

def queued_time_report(type, rd_data, client_data):
    rd_queued_time_max = rd_data["queued_time"].max()
    rd_queued_time_50_per = percentile(rd_data, "queued_time")[0]
    rd_queued_time_75_per = percentile(rd_data, "queued_time")[1]
    client_queued_time_max = client_data["queued_time"].max()
    client_queued_time_50_per = percentile(client_data, "queued_time")[0]
    client_queued_time_75_per = percentile(client_data, "queued_time")[1]
    print("RD {0:3s} job max queued time: {1:9.2f}  min".format(type, rd_queued_time_max))
    print("RD {0:3s} job 50% queued time: {1:9.2f}  min".format(type, rd_queued_time_50_per))
    print("RD {0:3s} job 75% queued time: {1:9.2f}  min".format(type, rd_queued_time_75_per))
    print("Client {0:3s} job max queued time: {1:9.2f}  min".format(type, client_queued_time_max))
    print("Client {0:3s} job 50% queued time: {1:9.2f}  min".format(type, client_queued_time_50_per))
    print("Client {0:3s} job 75% queued time: {1:9.2f}  min".format(type, client_queued_time_75_per))

def runtime_report(type, rd_data, client_data):
    rd_runtime_max = rd_data["running_time"].max()
    rd_runtime_50_per = percentile(rd_data, "running_time")[0]
    rd_runtime_75_per = percentile(rd_data, "running_time")[1]
    client_runtime_max = client_data["running_time"].max()
    client_runtime_50_per = percentile(client_data, "running_time")[0]
    client_runtime_75_per = percentile(client_data, "running_time")[1]
    print("RD {0:3s} job max running time: {1:9.2f}  min".format(type, rd_runtime_max))
    print("RD {0:3s} job 50% running time: {1:9.2f}  min".format(type, rd_runtime_50_per))
    print("RD {0:3s} job 75% running time: {1:9.2f}  min".format(type, rd_runtime_75_per))
    print("Client {0:3s} job max running time: {1:9.2f}  min".format(type, client_runtime_max))
    print("Client {0:3s} job 50% running time: {1:9.2f}  min".format(type, client_runtime_50_per))
    print("Client {0:3s} job 75% running time: {1:9.2f}  min".format(type, client_runtime_75_per))

def cpu_job_report(rd_data, client_data):
    rd_core_max = rd_data["cpu_num"].max()
    rd_core_50_per = int(percentile(rd_data, "cpu_num")[0])
    rd_core_75_per = int(percentile(rd_data, "cpu_num")[1])
    client_core_max = client_data["cpu_num"].max()
    client_core_50_per = int(percentile(client_data, "cpu_num")[0])
    client_core_75_per = int(percentile(client_data, "cpu_num")[1])
    print("RD job using max number of CPUs: {0:3d}".format(rd_core_max))
    print("50% of RD job using number of CPUs: {0:3d}".format(rd_core_50_per))
    print("75% of RD job using number of CPUs: {0:3d}".format(rd_core_75_per))
    print("Client job using max number of CPUs: {0:3d}".format(client_core_max))
    print("50% of Client job using number of CPUs: {0:3d}".format(client_core_50_per))
    print("75% of Client job using number of CPUs: {0:3d}".format(client_core_75_per))
    rd_cpuhour_max = rd_data["cpu_hour"].max()
    rd_cpuhour_50_per = percentile(rd_data, "cpu_hour")[0]
    rd_cpuhour_75_per = percentile(rd_data, "cpu_hour")[1]
    client_cpuhour_max = client_data["cpu_hour"].max()
    client_cpuhour_50_per = percentile(client_data, "cpu_hour")[0]
    client_cpuhour_75_per = percentile(client_data, "cpu_hour")[1]
    print("RD job using max number of CPU hours: {0:9.2f}".format(rd_cpuhour_max))
    print("50% of RD job using number of CPU hours: {0:9.2f}".format(rd_cpuhour_50_per))
    print("75% of RD job using number of CPU hours: {0:9.2f}".format(rd_cpuhour_75_per))
    print("Client job using max number of CPU hours: {0:9.2f}".format(client_cpuhour_max))
    print("50% of Client job using number of CPU hours: {0:9.2f}".format(client_cpuhour_50_per))
    print("75% of Client job using number of CPU hours: {0:9.2f}".format(client_cpuhour_75_per))

def gpu_job_report(rd_data, client_data):
    rd_core_max = rd_data["gpu_num"].max()
    rd_core_50_per = int(percentile(rd_data, "gpu_num")[0])
    rd_core_75_per = int(percentile(rd_data, "gpu_num")[1])
    client_core_max = client_data["gpu_num"].max()
    client_core_50_per = int(percentile(client_data, "gpu_num")[0])
    client_core_75_per = int(percentile(client_data, "gpu_num")[1])
    print("RD job using max number of GPUs: {0:3d}".format(rd_core_max))
    print("50% of RD job using number of GPUs: {0:3d}".format(rd_core_50_per))
    print("75% of RD job using number of GPUs: {0:3d}".format(rd_core_75_per))
    print("Client job using max number of GPUs: {0:3d}".format(client_core_max))
    print("50% of Client job using number of GPUs: {0:3d}".format(client_core_50_per))
    print("75% of Client job using number of GPUs: {0:3d}".format(client_core_75_per))
    rd_gpuhour_max = rd_data["gpu_hour"].max()
    rd_gpuhour_50_per = percentile(rd_data, "gpu_hour")[0]
    rd_gpuhour_75_per = percentile(rd_data, "gpu_hour")[1]
    client_gpuhour_max = client_data["gpu_hour"].max()
    client_gpuhour_50_per = percentile(client_data, "gpu_hour")[0]
    client_gpuhour_75_per = percentile(client_data, "gpu_hour")[1]
    print("RD job using max number of GPU hours: {0:9.2f}".format(rd_gpuhour_max))
    print("50% of RD job using number of GPU hours: {0:9.2f}".format(rd_gpuhour_50_per))
    print("75% of RD job using number of GPU hours: {0:9.2f}".format(rd_gpuhour_75_per))
    print("Client job using max number of GPU hours: {0:9.2f}".format(client_gpuhour_max))
    print("50% of Client job using number of GPU hours: {0:9.2f}".format(client_gpuhour_50_per))
    print("75% of Client job using number of GPU hours: {0:9.2f}".format(client_gpuhour_75_per))

def cpu_job_occupancy_report(rd_cpu_job_data, client_cpu_job_data):
    rd_cpu_occupancy_max = rd_cpu_job_data["cpu_occupancy"].max()
    rd_cpu_occupancy_50_per = percentile(rd_cpu_job_data, "cpu_occupancy")[0]
    rd_cpu_occupancy_75_per = percentile(rd_cpu_job_data, "cpu_occupancy")[1]
    client_cpu_occupancy_max = client_cpu_job_data["cpu_occupancy"].max()
    client_cpu_occupancy_50_per = percentile(client_cpu_job_data, "cpu_occupancy")[0]
    client_cpu_occupancy_75_per = percentile(client_cpu_job_data, "cpu_occupancy")[1]
    print("RD job max CPU occupancy: {0:5.2f} %".format(rd_cpu_occupancy_max))
    print("RD job 50% CPU occupancy: {0:5.2f} %".format(rd_cpu_occupancy_50_per))
    print("RD job 75% CPU occupancy: {0:5.2f} %".format(rd_cpu_occupancy_75_per))
    print("Client job max CPU occupancy: {0:5.2f} %".format(client_cpu_occupancy_max))
    print("Client job 50% CPU occupancy: {0:5.2f} %".format(client_cpu_occupancy_50_per))
    print("Client job 75% CPU occupancy: {0:5.2f} %".format(client_cpu_occupancy_75_per))

def gpu_job_occupancy_report(rd_gpu_job_data, client_gpu_job_data):
    rd_gpu_occupancy_max = rd_gpu_job_data["gpu_occupancy"].max()
    rd_gpu_occupancy_50_per = percentile(rd_gpu_job_data, "gpu_occupancy")[0]
    rd_gpu_occupancy_75_per = percentile(rd_gpu_job_data, "gpu_occupancy")[1]
    client_gpu_occupancy_max = client_gpu_job_data["gpu_occupancy"].max()
    client_gpu_occupancy_50_per = percentile(client_gpu_job_data, "gpu_occupancy")[0]
    client_gpu_occupancy_75_per = percentile(client_gpu_job_data, "gpu_occupancy")[1]
    print("RD job max GPU occupancy: {0:5.2f} %".format(rd_gpu_occupancy_max))
    print("RD job 50% GPU occupancy: {0:5.2f} %".format(rd_gpu_occupancy_50_per))
    print("RD job 75% GPU occupancy: {0:5.2f} %".format(rd_gpu_occupancy_75_per))
    print("Client job max GPU occupancy: {0:5.2f} %".format(client_gpu_occupancy_max))
    print("Client job 50% GPU occupancy: {0:5.2f} %".format(client_gpu_occupancy_50_per))
    print("Client job 75% GPU occupancy: {0:5.2f} %".format(client_gpu_occupancy_75_per))

def cpu_data_analysis(rd_cpu_data, client_cpu_data):
    queued_time_report("CPU", rd_cpu_data, client_cpu_data)
    runtime_report("CPU", rd_cpu_data, client_cpu_data)
    cpu_job_report(rd_cpu_data, client_cpu_data)

def gpu_data_analysis(rd_gpu_data, client_gpu_data):
    queued_time_report("GPU", rd_gpu_data, client_gpu_data)
    runtime_report("GPU", rd_gpu_data, client_gpu_data)
    gpu_job_report(rd_gpu_data, client_gpu_data)

def data_type_conversion(dataframe, job_type):
    dataframe.index = pd.to_datetime(dataframe["date"])
    dataframe["queued_time"] = pd.to_numeric(dataframe["queued_time"],downcast = "float")
    dataframe["running_time"] = pd.to_numeric(dataframe["running_time"],downcast = "float")
    if job_type == "CPU":
        dataframe["cpu_num"] = pd.to_numeric(dataframe["cpu_num"],downcast = "integer")
        dataframe["cpu_hour"] = pd.to_numeric(dataframe["cpu_hour"],downcast = "float")
    if job_type == "GPU":
        dataframe["gpu_num"] = pd.to_numeric(dataframe["gpu_num"],downcast = "integer")
        dataframe["gpu_hour"] = pd.to_numeric(dataframe["gpu_hour"],downcast = "float")
    return dataframe

def occupancy_data_conversion(dataframe, job_type):
    if job_type == "CPU":
        dataframe["cpu_occupancy"] = pd.to_numeric(dataframe["cpu_occupancy"],downcast = "float")
        dataframe.index = pd.to_datetime(dataframe["date"])
    else:
        dataframe["gpu_occupancy"] = pd.to_numeric(dataframe["gpu_occupancy"],downcast = "float")
        dataframe.index = pd.to_datetime(dataframe["date"])
    return dataframe

def daily_occupancy(dataframe,job_type):
    dataframe = dataframe.resample('D').sum()
    if job_type == "CPU":
        dataframe["cpu_occupancy"]=dataframe["cpu_occupancy"]/(24.0*80*16)*100
    else:
        dataframe["gpu_occupancy"]=dataframe["gpu_occupancy"]/(24.0*4*16)*100
    return dataframe

def main(begin, end):
    rd_cpu_info, rd_gpu_info, client_cpu_info, client_gpu_info,rd_cpu_occupancy, rd_gpu_occupancy, client_cpu_occupancy, client_gpu_occupancy = data_collection(begin, end)
    if len(rd_cpu_info) != 0:
        rd_cpu_data = np.array(rd_cpu_info)
        rd_cpu_data = pd.DataFrame(rd_cpu_data, columns=["date","account","queued_time","running_time","cpu_num","cpu_hour"])
        rd_cpu_data = data_type_conversion(rd_cpu_data, "CPU")
    if len(rd_gpu_info) != 0:
        rd_gpu_data = np.array(rd_gpu_info)
        rd_gpu_data = pd.DataFrame(rd_gpu_data, columns=["date","account","queued_time","running_time","gpu_num","gpu_hour"])
        rd_gpu_data = data_type_conversion(rd_gpu_data, "GPU")
    if len(client_cpu_info) != 0:
        client_cpu_data = np.array(client_cpu_info)
        client_cpu_data = pd.DataFrame(client_cpu_data, columns=["date","account","queued_time","running_time","cpu_num","cpu_hour"])
        client_cpu_data = data_type_conversion(client_cpu_data, "CPU")
    if len(client_gpu_info) != 0:
        client_gpu_data = np.array(client_gpu_info)
        client_gpu_data = pd.DataFrame(client_gpu_data, columns=["date","account","queued_time","running_time","gpu_num","gpu_hour"])
        client_gpu_data = data_type_conversion(client_gpu_data, "GPU")
    if len(rd_cpu_occupancy) != 0:
        rd_cpu_occupancy_data = np.array(rd_cpu_occupancy)
        rd_cpu_occupancy_data = pd.DataFrame(rd_cpu_occupancy_data, columns=["date","cpu_occupancy"])
        rd_cpu_occupancy_data = occupancy_data_conversion(rd_cpu_occupancy_data, "CPU")
        rd_cpu_daily_occupancy = daily_occupancy(rd_cpu_occupancy_data, "CPU")
    if len(rd_gpu_occupancy) != 0:
        rd_gpu_occupancy_data = np.array(rd_gpu_occupancy)
        rd_gpu_occupancy_data = pd.DataFrame(rd_gpu_occupancy_data, columns=["date","gpu_occupancy"])
        rd_gpu_occupancy_data = occupancy_data_conversion(rd_gpu_occupancy_data, "GPU")
        rd_gpu_daily_occupancy = daily_occupancy(rd_gpu_occupancy_data, "GPU")
    if len(client_cpu_occupancy) != 0:
        client_cpu_occupancy_data = np.array(client_cpu_occupancy)
        client_cpu_occupancy_data = pd.DataFrame(client_cpu_occupancy_data, columns=["date","cpu_occupancy"])
        client_cpu_occupancy_data = occupancy_data_conversion(client_cpu_occupancy_data, "CPU")
        client_cpu_daily_occupancy = daily_occupancy(client_cpu_occupancy_data, "CPU")
    if len(client_gpu_occupancy) != 0:
        client_gpu_occupancy_data = np.array(client_gpu_occupancy)
        client_gpu_occupancy_data = pd.DataFrame(client_gpu_occupancy_data, columns=["date","gpu_occupancy"])
        client_gpu_occupancy_data = occupancy_data_conversion(client_gpu_occupancy_data, "GPU")
        client_gpu_daily_occupancy = daily_occupancy(client_gpu_occupancy_data, "GPU")
    rd_cpu_data.to_csv('rd_cpu_job_info.csv')
    client_cpu_data.to_csv('client_cpu_job_info.csv')
    rd_gpu_data.to_csv('rd_gpu_job_info.csv')
    client_gpu_data.to_csv('client_gpu_job_info.csv')
    rd_cpu_daily_occupancy.to_csv('rd_cpu_daily_occupancy.csv')
    client_cpu_daily_occupancy.to_csv('client_cpu_daily_occupancy.csv')
    rd_gpu_daily_occupancy.to_csv('rd_gpu_daily_occupancy.csv')
    client_gpu_daily_occupancy.to_csv('client_gpu_daily_occupancy.csv')
    cpu_data_analysis(rd_cpu_data, client_cpu_data)
    gpu_data_analysis(rd_gpu_data, client_gpu_data)
    cpu_job_occupancy_report(rd_cpu_daily_occupancy,client_cpu_daily_occupancy)
    gpu_job_occupancy_report(rd_gpu_daily_occupancy,client_gpu_daily_occupancy)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to analyze cluster usage",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("start_time", help="start time")
    parser.add_argument("end_time", help="end time")
    args = vars(parser.parse_args())

    begin = args["start_time"]
    end = args["end_time"]
    main(begin, end)
