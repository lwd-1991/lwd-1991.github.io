// editor-worker.js
// Web Worker：负责流式读取本地 File 或通过 fetch 流式下载 URL，并把文本 chunk 发回主线程.
// 支持编码自动检测（初步 UTF-8 检测），并在检测失败时通知主线程以便提示用户尝试其他编码（如 GBK）。

self._tasks = new Map()

self.addEventListener('message', async (ev) => {
    const msg = ev.data
    try {
        if (msg.action === 'start') {
            const id = msg.id
            const task = msg.task
            const controller = new AbortController()
            self._tasks.set(id, { controller, state: { buffered: [], bufferedBytes: 0, encoding: task.encoding || 'auto' } })
            if (task.kind === 'file') {
                await streamFile(id, task.file, controller.signal)
            } else if (task.kind === 'url') {
                await streamURL(id, task.url, controller.signal)
            }
        } else if (msg.action === 'cancel') {
            const id = msg.id
            const t = self._tasks.get(id)
            if (t) { t.controller.abort(); self._tasks.delete(id); postMessage({ type:'error', message:'用户已取消', id }) }
        } else if (msg.action === 'resume-with-encoding') {
            const id = msg.id
            const encoding = msg.encoding || 'utf-8'
            const t = self._tasks.get(id)
            if (t) {
                t.state.encoding = encoding
                // resume reading using buffered bytes first
                if (t.state.buffered && t.state.buffered.length) {
                    // concatenate buffered
                    const totalBuf = concatUint8Arrays(t.state.buffered)
                    // decode all buffered with chosen encoding and emit
                    try {
                        const decoder = new TextDecoder(encoding)
                        const text = decoder.decode(totalBuf)
                        postMessage({ type:'chunk', text })
                    } catch (err) {
                        postMessage({ type:'error', message: '指定编码不可用: ' + encoding, id })
                        self._tasks.delete(id)
                        return
                    }
                    // clear buffers
                    t.state.buffered = []
                    t.state.bufferedBytes = 0
                    // now continue reading from stream -- stream functions expect to continue naturally; we rely on the existing reader loop to continue
                }
            }
        }
    } catch (err) {
        postMessage({ type:'error', message: err && err.message ? err.message : String(err) })
    }
})

function concatUint8Arrays(arrays) {
    const total = arrays.reduce((s, a) => s + a.length, 0)
    const out = new Uint8Array(total)
    let off = 0
    for (const a of arrays) { out.set(a, off); off += a.length }
    return out
}

async function streamFile(id, file, signal) {
    try {
        const total = file.size || 0
        const reader = file.stream().getReader()
        let read = 0
        const task = self._tasks.get(id)
        if (!task) throw new Error('task not found')
        const decoder = () => new TextDecoder('utf-8')

        while (true) {
            const { done, value } = await reader.read()
            if (signal.aborted) { reader.cancel(); throw new Error('aborted') }
            if (done) break
            // If encoding is auto and not yet determined, buffer and test
            if (task.state.encoding === 'auto') {
                task.state.buffered.push(value)
                task.state.bufferedBytes += value.length
                // try detect when we have enough bytes or every chunk
                const buf = concatUint8Arrays(task.state.buffered)
                try {
                    // try strict utf-8 decode using fatal flag
                    const dec = new TextDecoder('utf-8', { fatal: true })
                    const text = dec.decode(buf)
                    // success: emit text and switch to utf-8 streaming
                    postMessage({ type: 'chunk', text })
                    postMessage({ type: 'progress', ratio: total ? (read + task.state.bufferedBytes) / total : 0 })
                    task.state.buffered = []
                    task.state.bufferedBytes = 0
                    task.state.encoding = 'utf-8'
                } catch (err) {
                    // decoding failed -> notify main thread to choose encoding
                    postMessage({ type: 'encoding-issue', id })
                    // pause reading: wait until resume or cancel
                    // We'll wait by looping until encoding changed or aborted
                    while (task.state.encoding === 'auto') {
                        await new Promise((res) => setTimeout(res, 200))
                        if (signal.aborted) throw new Error('aborted')
                    }
                    // If encoding now set (e.g., gbk), decode buffered with that encoding
                    if (task.state.encoding !== 'utf-8') {
                        try {
                            const dec2 = new TextDecoder(task.state.encoding)
                            const text2 = dec2.decode(concatUint8Arrays(task.state.buffered))
                            postMessage({ type: 'chunk', text: text2 })
                            task.state.buffered = []
                            task.state.bufferedBytes = 0
                        } catch (err2) {
                            postMessage({ type:'error', message: '指定编码不可用: ' + task.state.encoding, id })
                            self._tasks.delete(id)
                            return
                        }
                    }
                }
                // continue
            } else {
                // known encoding
                try {
                    const dec = new TextDecoder(task.state.encoding)
                    const text = dec.decode(value, { stream: true })
                    read += value.byteLength
                    postMessage({ type: 'chunk', text })
                    postMessage({ type: 'progress', ratio: total ? (read / total) : 0 })
                } catch (err) {
                    postMessage({ type:'error', message: '解码出错: ' + (err.message || err), id })
                    self._tasks.delete(id)
                    return
                }
            }
        }
        postMessage({ type:'done', id })
        self._tasks.delete(id)
    } catch (err) {
        if (err.name === 'AbortError' || err.message === 'aborted') {
            postMessage({ type:'error', message:'加载已取消', id })
        } else {
            postMessage({ type:'error', message: err.message || String(err), id })
        }
        self._tasks.delete(id)
    }
}

async function streamURL(id, url, signal) {
    try {
        const resp = await fetch(url, { signal })
        if (!resp.ok) throw new Error('HTTP ' + resp.status)
        const contentLength = resp.headers.get('Content-Length')
        const total = contentLength ? parseInt(contentLength, 10) : 0
        const reader = resp.body.getReader()
        let read = 0
        const task = self._tasks.get(id)
        if (!task) throw new Error('task not found')

        while (true) {
            const { done, value } = await reader.read()
            if (signal.aborted) { reader.cancel(); throw new Error('aborted') }
            if (done) break
            if (task.state.encoding === 'auto') {
                task.state.buffered.push(value)
                task.state.bufferedBytes += value.length
                const buf = concatUint8Arrays(task.state.buffered)
                try {
                    const dec = new TextDecoder('utf-8', { fatal: true })
                    const text = dec.decode(buf)
                    postMessage({ type: 'chunk', text })
                    postMessage({ type: 'progress', ratio: total ? (read + task.state.bufferedBytes) / total : 0 })
                    task.state.buffered = []
                    task.state.bufferedBytes = 0
                    task.state.encoding = 'utf-8'
                } catch (err) {
                    postMessage({ type: 'encoding-issue', id })
                    while (task.state.encoding === 'auto') {
                        await new Promise((res) => setTimeout(res, 200))
                        if (signal.aborted) throw new Error('aborted')
                    }
                    if (task.state.encoding !== 'utf-8') {
                        try {
                            const dec2 = new TextDecoder(task.state.encoding)
                            const text2 = dec2.decode(concatUint8Arrays(task.state.buffered))
                            postMessage({ type: 'chunk', text: text2 })
                            task.state.buffered = []
                            task.state.bufferedBytes = 0
                        } catch (err2) {
                            postMessage({ type:'error', message: '指定编码不可用: ' + task.state.encoding, id })
                            self._tasks.delete(id)
                            return
                        }
                    }
                }
            } else {
                try {
                    const dec = new TextDecoder(task.state.encoding)
                    const text = dec.decode(value, { stream: true })
                    read += value.byteLength
                    postMessage({ type: 'chunk', text })
                    postMessage({ type: 'progress', ratio: total ? (read / total) : 0 })
                } catch (err) {
                    postMessage({ type:'error', message: '解码出错: ' + (err.message || err), id })
                    self._tasks.delete(id)
                    return
                }
            }
        }
        postMessage({ type:'done', id })
        self._tasks.delete(id)
    } catch (err) {
        if (err.name === 'AbortError' || err.message === 'aborted') {
            postMessage({ type:'error', message:'加载已取消', id })
        } else {
            postMessage({ type:'error', message: err.message || String(err), id })
        }
        self._tasks.delete(id)
    }
}
