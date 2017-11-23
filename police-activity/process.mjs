import fs from "fs"
import path from "path"
import stream from "stream"
import { getDatasetPath, SkipFirstLine, ColumnExtractor } from "../helpers.mjs"
import { TRAFFIC_ACCIDENTS } from "../datasets"

const annualCounts = new Map()
fs.createReadStream(getDatasetPath(TRAFFIC_ACCIDENTS))
  .pipe(new SkipFirstLine)
  .pipe(new ColumnExtractor([7]))
  .on(`data`, (data) => {
    const year = JSON.parse(data.toString())[0].slice(0, 4)
    if (!annualCounts.has(year)) {
      annualCounts.set(year, 0)
    }
    annualCounts.set(year, annualCounts.get(year) + 1)
  })
  .on(`end`, () => console.log(annualCounts))
  
const categoryCounts = new Map()
fs.createReadStream(getDatasetPath(TRAFFIC_ACCIDENTS))
  .pipe(new SkipFirstLine)
  .pipe(new ColumnExtractor([6,7]))
  .on(`data`, (data) => {
    const event = JSON.parse(data.toString())
    const year = event[1].slice(0, 4)
    const key = `${year} - ${event[0]}`
    if (!categoryCounts.has(key)) {
      categoryCounts.set(key, 0)
    }
    categoryCounts.set(key, categoryCounts.get(key) + 1)
  })
  .on(`end`, () => console.log(categoryCounts))

const neighborhoodCounts = new Map()
fs.createReadStream(getDatasetPath(TRAFFIC_ACCIDENTS))
  .pipe(new SkipFirstLine)
  .pipe(new ColumnExtractor([7,17, 6]))
  .on(`data`, (data) => {
    const event = JSON.parse(data.toString())
    const year = event[0].slice(0, 4)
    const fatal = event[2] === `TRAF - ACCIDENT - FATAL`
    if (year !== `2017`) {
      return
    } else if (!fatal) {
      return
    }
    const key = `${event[1]}`
    if (!neighborhoodCounts.has(key)) {
      neighborhoodCounts.set(key, 0)
    }
    neighborhoodCounts.set(key, neighborhoodCounts.get(key) + 1)
  })
  .on(`end`, () => console.log(neighborhoodCounts))
  
const yorkCounts = new Map()
fs.createReadStream(getDatasetPath(TRAFFIC_ACCIDENTS))
  .pipe(new SkipFirstLine)
  .pipe(new ColumnExtractor([6, 10, 7]))
  .on(`data`, (data) => {
    const event = JSON.parse(data.toString())
    if (event[1].indexOf(`N YORK ST`) === -1) {
      return
    }
    const year = event[2].slice(0, 4)
    const key = `N YORK ST - ${year} - ${event[0]}`
    if (!yorkCounts.has(key)) {
      yorkCounts.set(key, 0)
    }
    yorkCounts.set(key, yorkCounts.get(key) + 1)
  })
  .on(`end`, () => console.log(yorkCounts))
