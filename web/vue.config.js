/*
 * @Author: your name
 * @Date: 2021-12-10 10:17:25
 * @LastEditTime: 2021-12-13 11:49:37
 * @LastEditors: Please set LastEditors
 * @Description: 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 * @FilePath: \serve\vue.config.js
 */
module.exports = {
    publicPath:"/bdlog/",
    outputDir:"../bdlog/",
    devServer: {
        headers: {
            'Access-Control-Allow-Origin': '*'       
        },
        proxy: {
            '/bdatalog/list_history_all':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/modify_table_policy':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/list_table_field':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/list_table':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/list_history_by_name':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/unregister_table':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
            '/bdatalog/register_table':{
                target: 'http://127.0.0.1:8080',
                ws: true,
                changeOrigin: true
            },
        }
    }
}