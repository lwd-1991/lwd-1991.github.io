// editor-worker.js
// Web Worker：负责流式读取本地 File 或通过 fetch 流式下载 URL，并把文本 chunk 发回主线程.
// 支持取消：接收 { action: 'cancel', id }
// 主流程：postMessage({type:'chunk', text}) / {type:'progress', ratio} / {type:'done'} / {type:'error', message})

self._tasks = new Map()

self.addEventListener('message', async (ev) => {
    const msg = ev.data
    try {
        if (msg.action === 'start') {
            const id = msg.id
            const task = msg.task
            const controller = new AbortController()
            self._tasks.set(id, controller)
            if (task.kind === 'file') {
                await streamFile(id, task.file, controller.signal)
            } else if (task.kind === 'url') {
                await streamURL(id, task.url, controller.signal)
            }
            // done is posted by stream functions
        } else if (msg.action === 'cancel') {
            const id = msg.id
            const c = self._tasks.get(id)
            if (c) { c.abort(); self._tasks.delete(id); postMessage({ type:'error', message:'用户已取消', id }) }
        }
    } catch (err) {
        postMessage({ type:'error', message: err && err.message ? err.message : String(err) })
    }
})

async function streamFile(id, file, signal) {
    try {
        const total = file.size || 0
        const reader = file.stream().getReader()
        const decoder = new TextDecoder('utf-8')
        let read = 0
        while (true) {
            const { done, value } = await reader.read()
            if (signal.aborted) { reader.cancel(); throw new Error('aborted') }
            if (done) break
            const text = decoder.decode(value, { stream: true })
            read += value.byteLength
            postMessage({ type: 'chunk', text })
            postMessage({ type: 'progress', ratio: total ? (read / total) : 0 })
        }
        // flush any final bytes
        postMessage({ type:'done', id })
        self._tasks.delete(id)
    } catch (err) {
        if (err.name === 'AbortError' || err.message === 'aborted') {
            // cancellation is handled by main thread
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
        const decoder = new TextDecoder('utf-8')
        let read = 0
        while (true) {
            const { done, value } = await reader.read()
            if (signal.aborted) { reader.cancel(); throw new Error('aborted') }
            if (done) break
            const text = decoder.decode(value, { stream: true })
            read += value.byteLength
            postMessage({ type: 'chunk', text })
            postMessage({ type: 'progress', ratio: total ? (read / total) : 0 })
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
