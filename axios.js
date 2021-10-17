const fetchData = async url => {
    sleep.msleep(25)
    const response = await axios.get(url, { timeout: 300000 });
    success++;
    return response;
}

module.exports = fetchData;