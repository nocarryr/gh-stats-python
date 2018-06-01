$(function(){
    function zeroPad(s, ndigits){
        if (typeof(s) != 'string'){
            s = s.toString();
        }
        while (s.length < ndigits){
            s = '0' + s;
        }
        return s;
    }
    function updateDtPicker($el, value){
        var dateStr, timeStr;
        if (!value){
            return;
        }
        if (!(value instanceof Date)){
            value = new Date(value);
        }
        dateStr = [
            value.getFullYear().toString(),
            zeroPad(value.getMonth()+1, 2),
            zeroPad(value.getDate(), 2),
        ].join('-');
        timeStr = [
            zeroPad(value.getHours(), 2),
            zeroPad(value.getMinutes(), 2),
            '00',
            // zeroPad(value.getSeconds(), 2),
        ].join(':');
        $el.val([dateStr, timeStr].join('T'));
    }

    function getFormHiddenRepos($chart){
        var $form = $chart.data('form'),
            $el = $("input[name=hidden_repos]", $form),
            hiddenRepos = [];
        if ($el.val().length){
            $.each($el.val().split(','), function(i, repoSlug){
                hiddenRepos.push(repoSlug);
            });
        }
        $chart.data('hiddenRepos', hiddenRepos);
    }

    function checkChartCollides(evt, $chart){
        var ci = $chart.data('chart'),
            mouseXY = Chart.helpers.getRelativePosition(evt, ci),
            chartBounds = ci.chartArea;
        if (mouseXY.x < chartBounds.left){
            return false;
        }
        if (mouseXY.x > chartBounds.right){
            return false;
        }
        if (mouseXY.y < chartBounds.top){
            return false;
        }
        if (mouseXY.y > chartBounds.bottom){
            return false;
        }
        return true;
    }

    $("input[type=datetime-local]").each(function(){
        var $el = $(this),
            dt = new Date($el.data('value'));
        if (!$el.data('value')){
            return;
        }
        updateDtPicker($el, dt);
    });

    function loadChart($chart){
        var $form = $("form[data-chart-id=I]".replace('I', $chart.attr('id')));
        $chart.data('form', $form);
        getFormHiddenRepos($chart);
        $form.on("submit", function(e){
            var url = $form.attr('action'),
                q = $form.serialize();

            url = [url, q].join('?');
            e.preventDefault();
            $.getJSON(url, function(data){
                var el = $chart[0],
                    ctx = el.getContext('2d'),
                    chartOpts, chart;
                if (typeof($chart.data('chart')) != 'undefined'){
                    $chart.data('chart').destroy();
                    $chart.removeData('chart');
                }
                $chart.data('chart_data', data);
                $chart.data('hoverTarget', null);
                updateDtPicker($("[name=start_datetime]", $form), data.start_datetime);
                updateDtPicker($("[name=end_datetime]", $form), data.end_datetime);

                chartOpts = {
                    'type':'line',
                    'data':data.chart_data,
                    'options':{
                        onClick:function(e, elems){
                            var hoverTarget = $chart.data('hoverTarget');
                            if (hoverTarget === null){
                                return;
                            }
                            if (!checkChartCollides(e, $chart)){
                                return;
                            }
                            $chart.trigger('repoClicked', [hoverTarget]);
                        },
                        onHover:function(e, elems){
                            var index, itemKey;
                            if (elems.length != 1){
                                $chart.data('hoverTarget', null);
                                return;
                            }
                            index = elems[0]._datasetIndex;
                            itemKey = data.dataset_ids[index];
                            $chart.data('hoverTarget', itemKey);
                        },
                        responsive:true,
                        title:{
                            display:false,
                            text:'',
                        },
                        hover:{
                            mode:'nearest',
                            intersect:false,
                        },
                        tooltips:{
                            mode:'nearest',
                            intersect:false,
                        },
                        legend:{
                            display:true,
                            position:'right',
                            onClick: function(e, legendItem){
                                var index = legendItem.datasetIndex,
                                    ci = this.chart,
                                    meta = ci.getDatasetMeta(index),
                                    itemKey = data.dataset_ids[index];

                                meta.hidden = meta.hidden === null? !ci.data.datasets[index].hidden : null;
                                ci.update();
                                $chart.trigger('repoHidden', [itemKey, meta.hidden]);
                            },
                        },
                        scales:{
                            xAxes:[{
                                type: 'time',
                                distribution:'linear',
                                scaleLabel:{
                                    display:true,
                                    labelString:'Date'
                                },
                            }],
                            yAxes:[{
                                scaleLabel:{
                                    display:true,
                                    labelString:'value',
                                },
                            }],
                        },
                    },
                };
                chart = new Chart(ctx, chartOpts);
                $chart.data('chart', chart);
                $chart.on('repoHidden', function(e, repoSlug, hidden){
                    var hiddenRepos = $chart.data('hiddenRepos'),
                        $el = $("input[name=hidden_repos]", $form),
                        strVal;

                    if (hidden){
                        if (hiddenRepos.indexOf(repoSlug) == -1){
                            hiddenRepos.push(repoSlug);
                        }
                    } else {
                        if (hiddenRepos.indexOf(repoSlug) != -1){
                            hiddenRepos.splice(hiddenRepos.indexOf(repoSlug), 1);
                        }
                    }
                    $el.val(hiddenRepos.join(','));
                });
            });
        }).submit();
    }

    $(".chart").each(function(){
        var $chart = $(this);
        loadChart($chart);
    });
    $(".metric-select button").click(function(){
        var $btn = $(this),
            chartId = $btn.parents(".metric-select").data('chartId'),
            $form = $("form[data-chart-id=I]".replace('I', chartId)),
            $el = $("input[name=data_metric]", $form);
        if ($el.val() == $btn.data('metric')){
            return;
        }
        $el.val($btn.data('metric'));
        $form.submit();
    });
});
