package com.flink.demo;


import org.apache.directory.shared.kerberos.codec.apRep.actions.ApRepInit;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.example._21_Window_trigger_api_demo;

/**
 * A {@link Trigger} that fires once the watermark passes the end of the window to which a pane
 * belongs.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
@PublicEvolving
public class MyTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;


    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        _21_Window_trigger_api_demo.EventLog e=(_21_Window_trigger_api_demo.EventLog)element;
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else if(e.t().equals("2022-11-01 12:00:13.000")){
            System.out.println("--------e.t()==\"2022-11-01 12:00:13.000\"------");
            return TriggerResult.FIRE;
        } else{
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

}
