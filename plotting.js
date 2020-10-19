function removeElementsByClass(className){
    var elements = document.getElementsByClassName(className);
    while(elements.length > 0){
        elements[0].parentNode.removeChild(elements[0]);
    }
}

// a simple front end cache
var cache = {};
var plotting_data = {};

var layout = {
    yaxis: {
        title: {
            text: 'Number of bioRxiv articles',
        }
    }
};

document.getElementById("plot_button").addEventListener("click", function(e){
	e.preventDefault();
	// clear old plotting data
	plotting_data = {'data':[]};
	// show the beating heart
	document.getElementById('heart_wrapper').style.opacity = 1;
	// remove the old plot
	removeElementsByClass('plot-container')
	//get search terms from the input field
	var search_terms = document.getElementById("search_terms").value.replace(/, +/g,',').split(',')

	// send a POST request for each term individually (the api can handle multiple terms at time, but this speeds it up)
	plotting_data = {'data':[],'layout':layout};
	for(var i in search_terms){
		term = search_terms[i];
		// check if the term is in the front end cache
		if(term in cache){
			console.log('Cache')
			// do not make a POST request, just pull from the cache
			plotting_data['data'].push(cache[term])
			// only show the plot if all the search terms are ready
			if(plotting_data['data'].length == search_terms.length){
				document.getElementById('heart_wrapper').style.opacity = 0;
				Plotly.newPlot('plot',  plotting_data,{staticPlot: true});
			}
		}else{
			console.log('API')
			var data = JSON.stringify({ "terms": [term], "from_date": "","to_date": "" });
			$.post("https://q2whi6ss38.execute-api.us-east-2.amazonaws.com/prod/transactions",data,function(data, status){
				// if there are not results for the term, alert the user (otherwise nothing is plotted and it is just confusing)
				if('errorMessage' in data){
					alert('Well this is awkward...\nAn error has occurred while searching for "' + term + '"\nTry refreshing the page and searching for a new term');
					document.getElementById('heart_wrapper').style.opacity = 0;
				}
				if( data['data'][0]['x'].length == 0 ){
					alert(term + ' has no results');
				}
				data['data'][0]['mode'] = 'lines';
				plotting_data['data'].push(data['data'][0])
				// add term to the cache
				cache[term] = data['data'][0]
				document.getElementById('heart_wrapper').style.opacity = 0;
				console.log(plotting_data)
				Plotly.newPlot('plot',  plotting_data,config={'displayModeBar':false,staticPlot: true});
			});
		}
	}
});

$('#search_terms').css('color','#999');
$('#search_terms').css('font-size','16px');
$('#search_terms').focus(function(){
	if($('#search_terms').attr('value') == 'separate terms with commas'){
		$('#search_terms').attr('value','');
		$('#search_terms').css('color','black');
		$('#search_terms').css('font-size','30px');
	}
})
