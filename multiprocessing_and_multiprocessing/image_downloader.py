from threading import Thread
import requests
from os import path,mkdir,system
from shutil import rmtree
from time import sleep,perf_counter
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process,Pool
from tqdm import tqdm
import asyncio
system("clear")

DOWNLOAD_FOLDER = "images"
IMAGE_COUNT = 10
URl = "https://picsum.photos/2000/2000"
processes = []
download_times = {
    "Sequential": None,
    "Threading": None,
    "Threading.Pool": None,
    "Multiprocessing": None,
    "Multiprocessing.Pool": None
}

# Always start with a clean folder
def pre_setup():
    if path.exists(DOWNLOAD_FOLDER):
        # print(f"Folder {DOWNLOAD_FOLDER} already exists")
        rmtree(DOWNLOAD_FOLDER)
        # print(f"Creating folder {DOWNLOAD_FOLDER}")
        mkdir(DOWNLOAD_FOLDER)
        return
    # print(f"Creating folder {DOWNLOAD_FOLDER}")
    mkdir(DOWNLOAD_FOLDER)

# Main function to download the image
def download_image(image_name, url=URl):
    # print(f"Downloading image: {image_name}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        # Get the total file size from headers
        total_size = int(response.headers.get('content-length', 0))

        with open(f"{DOWNLOAD_FOLDER}/{image_name}.jpg", "wb") as file:
            with tqdm(
                    desc=f"{image_name}",
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=True  # Remove progress bar after completion
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        size = file.write(chunk)
                        pbar.update(size)

        # print(f"Image {image_name} downloaded")
    except Exception as e:
        print(f"Error downloading image {image_name}: {e}")

# Sequential Download
def download_sequentially():
    print("----------------------------------------")
    print("Downloading images sequentially")
    print("----------------------------------------")
    start_time = perf_counter()
    images = [f"image_{i+1}" for i in range(IMAGE_COUNT)]
    list(map(download_image,images))
    download_times["Sequential"] = (perf_counter() - start_time)

# Download with threads
def download_with_threads():
    print("----------------------------------------")
    print("Downloading images with threads")
    print("----------------------------------------")
    start_time = perf_counter()
    images = [f"image_{i + 1}" for i in range(IMAGE_COUNT)]
    threads = []
    for image in images:
        t = Thread(target=download_image, args=(image,),name=f"Thread-{image}")
        t.start()
        # print(f"Thread {index+1} started with name {t.name} and id {t.ident}")
        threads.append(t)
    for thread in threads:
        thread.join()
    download_times["Threading"] = (perf_counter() - start_time)

# Download with thread pool
def download_with_thread_pool():
    print("----------------------------------------")
    print("Downloading images with Thread Pool")
    print("----------------------------------------")
    start_time = perf_counter()
    with ThreadPoolExecutor(max_workers=IMAGE_COUNT) as executor:
        image_names = [f"image_{i + 1}" for i in range(IMAGE_COUNT)]
        for result in executor.map(download_image, image_names):
            pass
            # print(result)
    download_times["Threading.Pool"] = (perf_counter() - start_time)

# Download with multiprocessing
def download_with_multiprocessing():
    print("----------------------------------------")
    print("Downloading images with multiprocessing")
    print("----------------------------------------")
    images = [f"image_{i + 1}" for i in range(IMAGE_COUNT)]
    start_time = perf_counter()
    for image in images:
        p = Process(target=download_image, args=(image,),name=f"Process-{image}")
        p.start()
        # print(f"Process started with name {p.name} and id {p.pid}")
        processes.append(p)
    for process in processes:
        process.join()
    download_times["Multiprocessing"] = (perf_counter() - start_time)

# Download with multiprocessing pool
def download_with_multiprocessing_pool():
    print("----------------------------------------")
    print("Downloading images with Multiprocessing Pool")
    print("----------------------------------------")
    start_time = perf_counter()
    image_names = [f"image_{i+1}" for i in range(IMAGE_COUNT)]
    with Pool(processes=IMAGE_COUNT) as pool:  # Set number of worker processes
        results = pool.map(download_image, image_names)
        # for result in results:
            # print(result)
    download_times["Multiprocessing.Pool"] = (perf_counter() - start_time)

if __name__ == "__main__":
    # pre_setup()
    # download_sequentially()
    pre_setup()
    download_with_threads()
    pre_setup()
    download_with_thread_pool()
    pre_setup()
    download_with_multiprocessing()
    pre_setup()
    download_with_multiprocessing_pool()

    # Sort the dictionary by timing (ignoring None values for sorting)
    sorted_items = sorted(
        download_times.items(),
        key=lambda item: float(item[1]) if isinstance(item[1], float) else float('inf')
    )

    # Pretty print with 1 decimal place for floats
    print("Download Times (seconds):")
    print("---------------------------------------")
    for k, v in sorted_items:
        if isinstance(v, float):
            print(f"  {k:20}: {v:.1f}")
        else:
            print(f"  {k:20}: {v}")

    print("---------------------------------------")
    pre_setup()
