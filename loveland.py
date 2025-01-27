from selenium.webdriver.common.by import By

import local_common as common
import dbwriter
from pathlib import Path

from prefect import task, Flow
from prefect.cache_policies import NO_CACHE

@task(cache_policy=NO_CACHE)
def set_up_driver():
    return common.set_up_driver()

@task(cache_policy=NO_CACHE)
def connect(DRIVER):
    print("starting...")
    web = "https://skiloveland.com/trail-lift-report"
    DRIVER.get(web)
    
    # RUNS
    runs = []
    rows = DRIVER.find_elements(By.TAG_NAME, "tr")
    for run in rows:
        try:
            # DIFFICULTY
            if common.isElementPresent(run, By.XPATH, './/img[contains(@src, "beginner")]'):
                run_difficulty = "green"
            elif common.isElementPresent(run, By.XPATH, './/img[contains(@src, "more_difficult")]'):
                run_difficulty = "blue1"
            elif common.isElementPresent(run, By.XPATH, './/img[contains(@src, "most_difficult")]'):
                run_difficulty = "black1"
            elif common.isElementPresent(run, By.XPATH, './/img[contains(@src, "expert")]'):
                run_difficulty = "black2"
            elif common.isElementPresent(run, By.XPATH, './/img[contains(@src, "terrainpark")]'):
                run_difficulty = "terrainpark"
            else:
                continue

            # OPEN/CLOSE
            run_status = False
            if common.isElementPresent(run, By.XPATH, './/img[contains(@src, "open")]'):
                run_status=True

            # GROOMING
            run_groomed = False
            if common.isElementPresent(run, By.XPATH, './/img[contains(@src, "grooming")]'):
                run_groomed=True

            # NAME
            run_name = run.find_element(By.CLASS_NAME, "column-3").get_attribute("innerHTML")

            # AREA OF LOVELAND
            run_area = run.find_element(By.CLASS_NAME, "column-5").get_attribute("innerHTML")

            # print(f"{run_name}, {run_area}, {run_difficulty}, {run_groomed}, {run_status}")
            run_obj = {
                "runName": run_name,
                "runStatus": run_status,
                "runDifficulty": run_difficulty,
                "runArea": run_area,
                "runGroomed": run_groomed
            }
            runs.append(run_obj)
        except Exception as e:
            print("skipping run")

    # LIFTS
    lifts = []
    rows = DRIVER.find_elements(By.TAG_NAME, "h2")
    for row in rows:
        try:
            lift_name, lift_status_txt = row.get_attribute('innerHTML').split(' - ')
            lift_status = False
            if "OPEN" in lift_status_txt:
                lift_status = True
            lift_obj = {
                "liftName": lift_name,
                "liftStatus": lift_status,
            }
            lifts.append(lift_obj)
        except Exception as e:
            print("skipping lift")

    DRIVER.quit()
    return lifts, runs

@task
def export(lifts, runs, location):
    json_string = common.prepareForExport(lifts, runs, location)
    results = dbwriter.main(json_string)
    return results

@Flow
def main():
    DRIVER = set_up_driver()
    lifts, runs = connect(DRIVER)
    results = export(lifts, runs, "loveland")
    print(results)
    print("Done")

if __name__ == "__main__":
    main()
    main.from_source(
        source=str(Path(__file__).parent),
        entrypoint="loveland.py:main",
    ).deploy(
        name="corduroy-scraper-loveland",
        work_pool_name="local-pool",
        cron="0 13 * 10-12,1-5 *"
    )