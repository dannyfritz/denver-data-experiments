import https from "https"
import fs from "fs"
import path from "path"
import process from "process"
import stream from "stream"
const { Transform } = stream
const DATASET_LOCATION = path.resolve(`../datasets`)

export const getDatasetPath = (url) => {
  return path.join(DATASET_LOCATION, path.basename(url))
}

export const download = (url) => {
  https.get(url, (res) => {
    const { statusCode } = res
    let error
    const contentType = res.headers[`content-type`]
    if (statusCode !== 200) {
      error = new Error('Request Failed.\n' +
                        `Status Code: ${statusCode}`)
    }
    else if (!/^application\/octet-stream/.test(contentType)) {
      error = new Error('Invalid content-type.\n' +
                        `Expected application/octet-stream but received ${contentType}`)
    }
    if (error) {
      console.error(error.message)
      res.resume()
      return
    }
    res.setEncoding(`utf8`)
    const savePath = getDatasetPath(url)
    fs.closeSync(fs.openSync(savePath, 'w'))
    const writer = fs.createWriteStream(savePath)
    console.log(`Downloading... ${savePath}`)
    const contentLength = res.headers["content-length"]
    let size = 0
    res.on('data', (chunk) => { 
      try {
        writer.write(chunk)
        if (size > 0) process.stdout.write(`\b\b\b\b\b\b`)
        size += chunk.length
        const percentDone = size / contentLength * 100
        process.stdout.write(`${percentDone.toFixed(2).padStart(5, `0`)}%`)
      } catch (e) {
        console.error(e.message)
      }
    })
    res.on('end', () => {
      try {
        writer.end()
        process.stdout.write(`\b\b\b\b\b\b\b`)
        console.log("Done downloading!")
      } catch (e) {
        console.error(e.message)
      }
    })
  }).on('error', (e) => {
    console.error(`Got error: ${e.message}`)
  })
}

export class SkipFirstLine extends Transform {
  constructor () {
    super()
    this.skipped = false
    this.buffer = ""
  }
  _transform (data, encoding, processed) {
    let str = data.toString()
    if (!this.skipped)  {
      if (str.indexOf(`\n`) !== -1) {
        str = str.slice(str.indexOf('\n') + 1)
        this.skipped = true
      } else {
        processed()
        return
      }
    }
    this.push(str)
    processed()
  }
}

export class ColumnExtractor extends Transform {
  constructor (columnsArray) {
    super()
    this.columnsArray = columnsArray
    this.buffer = ""
  }
  _transform (data, encoding, processed) {
    const lines = (this.buffer + data.toString()).split(`\n`)
    while (lines.length > 1) {
      const line = lines.shift()
      const columns = line.split(`,`)
      const data = []
      for (const column of this.columnsArray) {
        data.push(columns[column].trim())
      }
      this.push(`${JSON.stringify(data)}\n`)
    }
    this.buffer += lines.join(``)
    processed();
  }
}
