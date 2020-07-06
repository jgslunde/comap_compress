# filename, gzip level, oldsize, newsize, ratio, compression time in minutes.
datalist = [
["comap-0014158-2020-06-11-132655.hd5", 1, 54.69, 30.90, 1.77, 52.4],
["comap-0014122-2020-06-10-083154.hd5", 1, 57.11, 32.11, 1.78, 56.0],
["comap-0014158-2020-06-11-132655.hd5", 2, 54.69, 30.35, 1.80, 56.4],
["comap-0013865-2020-06-01-101139.hd5", 1, 56.96, 32.99, 1.73, 56.5],
["comap-0014148-2020-06-11-050935.hd5", 1, 59.74, 33.59, 1.78, 57.0],
["comap-0014481-2020-06-24-205832.hd5", 1, 60.39, 33.99, 1.78, 57.6],
["comap-0013865-2020-06-01-101139.hd5", 2, 56.96, 32.92, 1.73, 59.0],
["comap-0014122-2020-06-10-083154.hd5", 2, 57.11, 31.54, 1.81, 59.9],
["comap-0014148-2020-06-11-050935.hd5", 2, 59.74, 32.97, 1.81, 61.4],
["comap-0014481-2020-06-24-205832.hd5", 2, 60.39, 33.38, 1.81, 63.3],
["comap-0014158-2020-06-11-132655.hd5", 4, 54.69, 29.38, 1.86, 67.8],
["comap-0014122-2020-06-10-083154.hd5", 4, 57.11, 30.52, 1.87, 70.5],
["comap-0014148-2020-06-11-050935.hd5", 4, 59.74, 31.88, 1.87, 72.3],
["comap-0013865-2020-06-01-101139.hd5", 4, 56.96, 32.65, 1.74, 72.7],
["comap-0014481-2020-06-24-205832.hd5", 4, 60.39, 32.32, 1.87, 73.2],
["comap-0014158-2020-06-11-132655.hd5", 3, 54.69, 28.75, 1.90, 73.5],
["comap-0013865-2020-06-01-101139.hd5", 3, 56.96, 32.14, 1.77, 74.4],
["comap-0014122-2020-06-10-083154.hd5", 3, 57.11, 29.85, 1.91, 75.5],
["comap-0014481-2020-06-24-205832.hd5", 3, 60.39, 31.63, 1.91, 79.4],
["comap-0014148-2020-06-11-050935.hd5", 3, 59.74, 31.19, 1.92, 80.0],
["comap-0014158-2020-06-11-132655.hd5", 5, 54.69, 29.00, 1.89, 101.0],
["comap-0013865-2020-06-01-101139.hd5", 5, 56.96, 32.40, 1.76, 101.1],
["comap-0014122-2020-06-10-083154.hd5", 5, 57.11, 30.11, 1.90, 107.0],
["comap-0014481-2020-06-24-205832.hd5", 5, 60.39, 31.90, 1.89, 111.4],
["comap-0014148-2020-06-11-050935.hd5", 5, 59.74, 31.45, 1.90, 111.8]]

datalist2 = [
["comap-0014158-2020-06-11-132655.hd5", 1, 420],
["comap-0014122-2020-06-10-083154.hd5", 1, 451],
["comap-0014158-2020-06-11-132655.hd5", 2, 389],
["comap-0014481-2020-06-24-205832.hd5", 1, 488],
["comap-0013865-2020-06-01-101139.hd5", 1, 464],
["comap-0014148-2020-06-11-050935.hd5", 1, 473],
["comap-0014481-2020-06-24-205832.hd5", 2, 450],
["comap-0014148-2020-06-11-050935.hd5", 2, 441],
["comap-0014122-2020-06-10-083154.hd5", 2, 409],
["comap-0013865-2020-06-01-101139.hd5", 2, 430],
["comap-0014481-2020-06-24-205832.hd5", 3, 394],
["comap-0014158-2020-06-11-132655.hd5", 3, 342],
["comap-0014148-2020-06-11-050935.hd5", 3, 381],
["comap-0014122-2020-06-10-083154.hd5", 3, 367],
["comap-0013865-2020-06-01-101139.hd5", 3, 393],
["comap-0014481-2020-06-24-205832.hd5", 4, 430],
["comap-0014158-2020-06-11-132655.hd5", 4, 373],
["comap-0014148-2020-06-11-050935.hd5", 4, 412],
["comap-0014122-2020-06-10-083154.hd5", 4, 398],
["comap-0013865-2020-06-01-101139.hd5", 4, 443],
["comap-0014481-2020-06-24-205832.hd5", 5, 405],
["comap-0014158-2020-06-11-132655.hd5", 5, 373],
["comap-0014148-2020-06-11-050935.hd5", 5, 404],
["comap-0014122-2020-06-10-083154.hd5", 5, 372],
["comap-0013865-2020-06-01-101139.hd5", 5, 433]]

import numpy as np
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size':16})

data = np.array([[asdf[1], asdf[4], asdf[5]] for asdf in datalist])
data2 = np.array([[asdf[1], asdf[2]] for asdf in datalist2])
data2[2] /= 60.0
N = data.shape[0]
data_mean = np.zeros((5, 3))
data_mean2 = np.zeros((5,2))
for i in range(5):
    gzip = i+1
    data_mean[i] = np.mean(data[data[:,0]==gzip], axis=0)
    data_mean2[i] = np.mean(data2[data2[:,0]==gzip], axis=0)
print(data_mean)
print(data_mean2)

def plot1():
    plt.scatter(data[:,0], data[:,2], c=data[:,0], alpha=0.2, s=80, cmap=plt.cm.get_cmap('Set1', 5))
    plt.scatter(data_mean[:,0], data_mean[:,2], c=data_mean[:,0], s=80, cmap=plt.cm.get_cmap('Set1', 5), vmin=0.5, vmax=5.5)
    plt.xlabel("GZIP level")
    plt.ylabel("Compression time [minutes]")
    plt.xticks([1,2,3,4,5])
    plt.colorbar(label="GZIP level")
    plt.tight_layout()
    plt.savefig("gzip_comptime.png", bbox_inches="tight")
    plt.clf()
plot1()

def plot2():
    plt.scatter(data[:,0], data[:,1], c=data[:,0], alpha=0.2, s=80, cmap=plt.cm.get_cmap('Set1', 5))
    plt.scatter(data_mean[:,0], data_mean[:,1], c=data_mean[:,0], s=80, cmap=plt.cm.get_cmap('Set1', 5), vmin=0.5, vmax=5.5)
    plt.xlabel("GZIP level")
    plt.ylabel("Compression ratio")
    plt.ylim(1.65, 1.95)
    plt.xticks([1,2,3,4,5])
    plt.colorbar(label="GZIP level")
    plt.tight_layout()
    plt.savefig("gzip_compratio.png", bbox_inches="tight")
    plt.clf()
plot2()

def plot3():
    plt.scatter(data2[:,0], data2[:,1], c=data2[:,0], alpha=0.2, s=80, cmap=plt.cm.get_cmap('Set1', 5))
    plt.scatter(data_mean2[:,0], data_mean2[:,1], c=data_mean2[:,0], s=80, cmap=plt.cm.get_cmap('Set1', 5), vmin=0.5, vmax=5.5)
    plt.xlabel("GZIP level")
    plt.ylabel("Decompression time [seconds]")
    plt.xticks([1,2,3,4,5])
    plt.ylim(300, 520)
    plt.colorbar(label="GZIP level")
    plt.tight_layout()
    plt.savefig("gzip_decomptime.png", bbox_inches="tight")
    plt.clf()
plot3()

def plot4():
    plt.scatter(data[:,2], data[:,1], c=data[:,0], alpha=0.2, s=80, cmap=plt.cm.get_cmap('Set1', 5))
    plt.scatter(data_mean[:,2], data_mean[:,1], c=data_mean[:,0], s=80, cmap=plt.cm.get_cmap('Set1', 5), vmin=0.5, vmax=5.5)
    plt.xlabel("Compression time [minutes]")
    plt.ylabel("Compression ratio")
    plt.ylim(1.65, 1.95)
    plt.xlim(45, 120)
    plt.colorbar(label="GZIP level")
    plt.tight_layout()
    plt.savefig("comptime_compratio.png", bbox_inches="tight")
    plt.clf()
plot4()